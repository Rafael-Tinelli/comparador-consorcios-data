#!/usr/bin/env python3
"""Build canônico da release V2 de dados, sem artefatos editoriais/SEO.

A lógica de transformação permanece em ``build_read_models_v2``. Este orquestrador
é o executável canônico dos workflows 11/12 e delimita a responsabilidade do
repositório: produzir somente contratos de dados auditáveis. Title, description,
canonical, breadcrumbs, FAQ e demais decisões editoriais pertencem ao frontend/site.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List

import build_read_models_v2 as core

PIPELINE_VERSION = "4.1.0"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build canônico data-only do Comparador de Consórcios V2"
    )
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

    # A V2 data-only não publica SEO. A remoção aqui também limpa artefatos
    # residuais de releases V2 anteriores quando o workflow 12 gerar o próximo
    # commit canônico.
    legacy_seo_dir = dist_base / "seo"
    removed_legacy_seo = legacy_seo_dir.exists()
    if removed_legacy_seo:
        shutil.rmtree(legacy_seo_dir)

    cadastro = stage_base / "cadastro" / "instituicoes_cadastro.json"
    filiais = stage_base / "filiais" / "filiais.json"
    monthly = raw_base / "bc" / "consorciobd" / "latest_source.bin"
    ranking = raw_base / "bc" / "ranking_reclamacoes" / "latest_source.csv"
    methodology_path = Path(args.methodology)

    required: List[Path] = [cadastro, filiais, monthly, ranking, methodology_path]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise SystemExit("Insumos V2 obrigatórios ausentes: " + ", ".join(missing))

    if methodology.get("general_score", {}).get("enabled") is not False:
        raise SystemExit("methodology_v2 deve manter general_score.enabled=false nesta versão")

    fingerprint = core.source_fingerprint(required, PIPELINE_VERSION)
    generated_at = core.stable_generated_at(global_dir / "meta.json", fingerprint)

    registry = core.load_registry(cadastro)
    branch_map, branch_unresolved = core.load_branches(filiais)
    rankings, ranking_meta = core.load_rankings_v2(ranking)
    products, monthly_meta = core.load_monthly_v2(monthly)
    models = core.build_models(registry, branch_map, rankings, products, generated_at)
    core.validate_models(models, monthly_meta)

    models["ofertas"] = {
        "metadata": {
            "generated_at": generated_at,
            "contract": core.CONTRACTS["ofertas"],
            "count": 0,
        },
        "items": [],
        "note": (
            "Ofertas comerciais permanecem separadas das evidências institucionais "
            "e não alteram comparação."
        ),
    }

    # ``seo`` permanece como família vazia somente para compatibilidade do
    # envelope operacional HostGator V2 já homologado. Nenhum arquivo SEO é
    # gerado, declarado ou baixado pela release data-only.
    artifact_entries: Dict[str, List[Dict[str, Any]]] = {"global": [], "seo": []}
    for name, payload in models.items():
        text = core.dump_json_text(payload)
        raw = text.encode("utf-8")
        artifact_entries["global"].append(
            {
                "file": f"{name}.json",
                "sha256": hashlib.sha256(raw).hexdigest(),
                "size_bytes": len(raw),
            }
        )

    registry_roots = {x["cnpj_root"] for x in registry}
    meta = {
        "generated_at": generated_at,
        "pipeline_version": PIPELINE_VERSION,
        "source_fingerprint": fingerprint,
        "methodology_version": methodology.get("version"),
        "methodology_sha256": hashlib.sha256(methodology_path.read_bytes()).hexdigest(),
        "contracts": core.CONTRACTS,
        "methodology": {
            "purpose": methodology.get("purpose"),
            "general_score": False,
            "missingness_policy": (
                "ausência não recebe zero, nota neutra nem redistribuição de peso"
            ),
            "presence_role": "informativo",
            "commercial_offers_affect_assessment": False,
        },
        "release_scope": {
            "kind": "data_only",
            "seo_artifacts": False,
            "seo_owner": "frontend_site",
        },
        "source_periods": {"consorciobd_mensal": monthly_meta.get("competencia")},
        "counts": {
            name: len(payload.get("items", [])) for name, payload in models.items()
        },
        "quality": {
            "registry_roots": len(registry),
            "monthly_consolidated_rows": monthly_meta.get("consolidated_rows_latest"),
            "monthly_operational_rows": monthly_meta.get("operational_rows_latest"),
            "monthly_consolidated_roots": monthly_meta.get("consolidated_roots_latest"),
            "monthly_operational_roots": monthly_meta.get("operational_roots_latest"),
            "ranking_rows": ranking_meta.get("row_count"),
            "branch_unresolved": len(branch_unresolved),
            "operational_orphans": sorted(
                {p["cnpj_root"] for p in products} - registry_roots
            ),
            "ranking_orphans": sorted(
                {r["cnpj_root"] for r in rankings} - registry_roots
            ),
        },
        "artifacts": artifact_entries,
        "notes": [
            "meta.json não contém auto-hash.",
            (
                "Todos os demais JSONs consumíveis produzidos por este builder "
                "constam no manifesto com SHA-256 dos bytes serializados."
            ),
            (
                "A release V2 é data-only: SEO editorial, rotas públicas, canonical, "
                "titles, descriptions, breadcrumbs e FAQ pertencem ao frontend/site."
            ),
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
        "seo_artifacts": False,
    }
    core.write_json_if_changed(runtime_dir / "build_read_models_v2.json", runtime)

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as fh:
            fh.write(f"changed={'true' if changed else 'false'}\n")
            fh.write("mode_used=build-release-v2-data-only\n")
            fh.write(f"records={len(models['administradoras']['items'])}\n")

    print(
        json.dumps(
            {
                "changed": changed,
                "mode_used": "build-release-v2-data-only",
                "records": len(models["administradoras"]["items"]),
                "products_observed": len(products),
                "competencia": monthly_meta.get("competencia"),
                "seo_artifacts": False,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
