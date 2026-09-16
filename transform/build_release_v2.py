#!/usr/bin/env python3
"""Build canônico data-only do Comparador de Consórcios V2."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List

import read_models_v2_core as core
import interpretation_v2 as interpretation
import segment_taxonomy_v2 as segment_taxonomy

PIPELINE_VERSION = "4.3.0"


def main() -> int:
    parser = argparse.ArgumentParser(description="Build canônico data-only do Comparador de Consórcios V2")
    parser.add_argument("--config", required=True)
    parser.add_argument("--methodology", required=True)
    args = parser.parse_args()

    cfg = core.load_json(Path(args.config))
    methodology = core.load_json(Path(args.methodology))
    defaults = cfg.get("defaults", {})
    raw_base = Path(defaults.get("raw_base_dir", "data/raw"))
    stage_base = Path(defaults.get("stage_base_dir", "data/stage"))
    dist_base = Path(defaults.get("dist_base_dir", "data/dist-v2"))
    global_dir = dist_base / "global"
    runtime_dir = Path("data/runtime")
    global_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    # Releases V2 são estritamente data-only. Qualquer resíduo SEO é removido.
    legacy_seo_dir = dist_base / "seo"
    removed_legacy_seo = legacy_seo_dir.exists()
    if removed_legacy_seo:
        shutil.rmtree(legacy_seo_dir)

    cadastro = stage_base / "cadastro" / "instituicoes_cadastro.json"
    filiais = stage_base / "filiais" / "filiais.json"
    monthly = raw_base / "bc" / "consorciobd" / "latest_source.bin"
    taxonomy_path = stage_base / "consorciobd" / "segment_taxonomy.json"
    ranking = raw_base / "bc" / "ranking_reclamacoes" / "latest_source.csv"
    methodology_path = Path(args.methodology)
    required: List[Path] = [
        cadastro,
        filiais,
        monthly,
        taxonomy_path,
        ranking,
        methodology_path,
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise SystemExit("Insumos V2 obrigatórios ausentes: " + ", ".join(missing))

    if methodology.get("general_score", {}).get("enabled") is not False:
        raise SystemExit("methodology_v2 deve manter general_score.enabled=false")
    relative = methodology.get("relative_interpretation", {})
    if relative.get("enabled") is not True:
        raise SystemExit("methodology_v2 deve habilitar relative_interpretation")
    if relative.get("contract") != interpretation.INTERPRETATION_CONTRACT:
        raise SystemExit("Contrato de interpretação relativa incompatível")
    if relative.get("general_ranking") is not False:
        raise SystemExit("relative_interpretation deve manter general_ranking=false")

    fingerprint = core.source_fingerprint(required, PIPELINE_VERSION)
    generated_at = core.stable_generated_at(global_dir / "meta.json", fingerprint)

    taxonomy = segment_taxonomy.load_taxonomy(taxonomy_path)
    observed_segment_codes = segment_taxonomy.observed_segment_codes(monthly)
    segment_taxonomy.assert_release_safe(taxonomy, observed_segment_codes)

    # A fonte oficial, e não o código da aplicação, passa a definir os segmentos válidos.
    # Keys já publicadas permanecem estáveis no arquivo versionado; códigos novos oficiais
    # entram automaticamente e passam a propagar por produtos, portfolios e comparações.
    core.SEGMENTS = segment_taxonomy.segment_map(taxonomy)

    registry = core.load_registry(cadastro)
    branch_map, branch_unresolved = core.load_branches(filiais)
    rankings, ranking_meta = core.load_rankings_v2(ranking)
    products, monthly_meta = core.load_monthly_v2(monthly)
    models = core.build_models(registry, branch_map, rankings, products, generated_at)
    core.validate_models(models, monthly_meta)

    taxonomy_meta = segment_taxonomy.taxonomy_summary(taxonomy, observed_segment_codes)
    models["segmentos"]["metadata"]["taxonomy_contract"] = segment_taxonomy.TAXONOMY_CONTRACT
    models["segmentos"]["metadata"]["taxonomy_source"] = taxonomy.get("source")
    models["segmentos"]["metadata"]["taxonomy_status"] = taxonomy.get("status")
    models["segmentos"]["metadata"]["official_codes"] = taxonomy_meta["known_codes"]

    models = interpretation.enrich_models(models, methodology)
    interpretation.validate_interpretations(models, methodology)
    models["ofertas"] = {
        "metadata": {"generated_at": generated_at, "contract": core.CONTRACTS["ofertas"], "count": 0},
        "items": [],
        "note": "Ofertas comerciais permanecem separadas das evidências institucionais e não alteram comparação.",
    }

    artifact_entries: Dict[str, List[Dict[str, Any]]] = {"global": [], "seo": []}
    for name, payload in models.items():
        raw = core.dump_json_text(payload).encode("utf-8")
        artifact_entries["global"].append({
            "file": f"{name}.json",
            "sha256": hashlib.sha256(raw).hexdigest(),
            "size_bytes": len(raw),
        })

    registry_roots = {x["cnpj_root"] for x in registry}
    meta = {
        "generated_at": generated_at,
        "pipeline_version": PIPELINE_VERSION,
        "source_fingerprint": fingerprint,
        "methodology_version": methodology.get("version"),
        "methodology_sha256": hashlib.sha256(methodology_path.read_bytes()).hexdigest(),
        "contracts": core.CONTRACTS,
        "embedded_contracts": {
            "interpretacao_relativa": interpretation.INTERPRETATION_CONTRACT,
            "segment_taxonomy": segment_taxonomy.TAXONOMY_CONTRACT,
        },
        "methodology": {
            "purpose": methodology.get("purpose"),
            "general_score": False,
            "missingness_policy": "ausência não recebe zero, nota neutra nem redistribuição de peso",
            "presence_role": "informativo",
            "commercial_offers_affect_assessment": False,
            "relative_interpretation": {
                "enabled": True,
                "contract": interpretation.INTERPRETATION_CONTRACT,
                "general_ranking": False,
                "comparison_unit": relative.get("comparison_unit"),
                "complaints_scope": relative.get("complaints_scope"),
                "quartile_method": relative.get("quartile_method"),
                "contemplation_policy": relative.get("contemplation_policy"),
            },
        },
        "segment_taxonomy": taxonomy_meta,
        "release_scope": {"kind": "data_only", "seo_artifacts": False, "seo_owner": "frontend_site"},
        "source_periods": {"consorciobd_mensal": monthly_meta.get("competencia")},
        "counts": {name: len(payload.get("items", [])) for name, payload in models.items()},
        "quality": {
            "registry_roots": len(registry),
            "monthly_consolidated_rows": monthly_meta.get("consolidated_rows_latest"),
            "monthly_operational_rows": monthly_meta.get("operational_rows_latest"),
            "monthly_consolidated_roots": monthly_meta.get("consolidated_roots_latest"),
            "monthly_operational_roots": monthly_meta.get("operational_roots_latest"),
            "segment_codes_known": taxonomy_meta["known_codes"],
            "segment_codes_observed": taxonomy_meta["observed_codes"],
            "ranking_rows": ranking_meta.get("row_count"),
            "branch_unresolved": len(branch_unresolved),
            "operational_orphans": sorted({p["cnpj_root"] for p in products} - registry_roots),
            "ranking_orphans": sorted({r["cnpj_root"] for r in rankings} - registry_roots),
        },
        "artifacts": artifact_entries,
        "notes": [
            "meta.json não contém auto-hash.",
            "Todos os JSONs consumíveis não-meta constam no manifesto com SHA-256 e tamanho.",
            "SEO editorial pertence exclusivamente ao frontend/site.",
            "Novos códigos de segmento publicados pela fonte oficial entram automaticamente na camada de dados; novas páginas editoriais não são criadas pelo backend.",
            "Remoção ou mudança de significado de código oficial existente exige revisão humana.",
            "interpretacao-relativa.v1 não é score geral, ranking geral nem oferta comercial.",
        ],
    }

    changed = removed_legacy_seo
    for name, payload in models.items():
        changed |= core.write_json_if_changed(global_dir / f"{name}.json", payload)
    changed |= core.write_json_if_changed(global_dir / "meta.json", meta)

    runtime = {
        "last_checked_at": core.utc_now_iso(),
        "last_changed_at": core.utc_now_iso() if changed else None,
        "changed": changed,
        "mode_used": "build-release-v2-data-only",
        "pipeline_version": PIPELINE_VERSION,
        "source_fingerprint": fingerprint,
        "competencia": monthly_meta.get("competencia"),
        "operational_rows": monthly_meta.get("operational_rows_latest"),
        "segment_taxonomy_status": taxonomy.get("status"),
        "segment_codes": taxonomy_meta["known_codes"],
        "seo_artifacts": False,
        "interpretation_contract": interpretation.INTERPRETATION_CONTRACT,
    }
    core.write_json_if_changed(runtime_dir / "build_read_models_v2.json", runtime)

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as fh:
            fh.write(f"changed={'true' if changed else 'false'}\n")
            fh.write("mode_used=build-release-v2-data-only\n")
            fh.write(f"records={len(models['administradoras']['items'])}\n")

    print(json.dumps({
        "changed": changed,
        "mode_used": "build-release-v2-data-only",
        "records": len(models["administradoras"]["items"]),
        "products_observed": len(products),
        "competencia": monthly_meta.get("competencia"),
        "segment_taxonomy_status": taxonomy.get("status"),
        "segment_codes": taxonomy_meta["known_codes"],
        "seo_artifacts": False,
        "interpretation_contract": interpretation.INTERPRETATION_CONTRACT,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
