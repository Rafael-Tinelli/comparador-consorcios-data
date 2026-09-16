#!/usr/bin/env python3
"""Validate a generated Comparador de Consórcios V2 data-only release.

Structural validation is suitable for recurring production builds. An optional audit
baseline adds snapshot-specific assertions for regression review without freezing the
scheduled production pipeline to historical counts.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

EXPECTED_CONTRACTS = {
    "instituicoes.json": "instituicoes.v2",
    "administradoras.json": "administradoras.v2",
    "produtos.json": "produtos.v2",
    "rankings.json": "rankings.v2",
    "segmentos.json": "segmentos.v2",
    "comparacoes.json": "comparacoes.v2",
    "ofertas.json": "ofertas.v2",
}
RELEASE_CONTRACT = "comparador-v2-release.v2"
TAXONOMY_CONTRACT = "segment-taxonomy.v1"
SAFE_TAXONOMY_STATUS = {"ok", "ok_with_additions"}


def load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def fail(errors: List[str]) -> None:
    if errors:
        raise SystemExit("Validação V2 falhou:\n- " + "\n- ".join(errors))


def contract_of(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return None
    metadata = payload.get("metadata")
    if isinstance(metadata, dict) and metadata.get("contract"):
        return metadata.get("contract")
    return payload.get("contract")


def main() -> int:
    parser = argparse.ArgumentParser(description="Valida release V2 data-only gerada")
    parser.add_argument("--dist-base", required=True)
    parser.add_argument("--provenance-config", required=True)
    parser.add_argument("--deploy-config")
    parser.add_argument("--audit-baseline")
    args = parser.parse_args()

    root = Path(args.dist_base)
    g = root / "global"
    legacy_seo = root / "seo"
    errors: List[str] = []

    meta_path = g / "meta.json"
    if not meta_path.exists():
        fail([f"meta ausente: {meta_path}"])
    meta = load(meta_path)

    release_scope = meta.get("release_scope")
    if not isinstance(release_scope, dict) or release_scope.get("kind") != "data_only":
        errors.append("meta.release_scope.kind deve ser data_only")
    elif release_scope.get("seo_artifacts") is not False:
        errors.append("meta.release_scope.seo_artifacts deve ser false")
    if isinstance(release_scope, dict) and release_scope.get("seo_owner") != "frontend_site":
        errors.append("meta.release_scope.seo_owner deve ser frontend_site")

    payloads: Dict[str, Any] = {}
    for filename, expected in EXPECTED_CONTRACTS.items():
        path = g / filename
        if not path.exists():
            errors.append(f"artefato global ausente: {filename}")
            continue
        payload = load(path)
        payloads[filename] = payload
        if contract_of(payload) != expected:
            errors.append(f"contrato divergente em {filename}: {contract_of(payload)!r} != {expected!r}")

    fail(errors)
    errors = []

    admins = payloads["administradoras.json"].get("items", [])
    products = payloads["produtos.json"].get("items", [])
    rankings = payloads["rankings.json"].get("items", [])
    segments = payloads["segmentos.json"].get("items", [])
    comparisons = payloads["comparacoes.json"].get("items", [])

    if len(admins) < 100:
        errors.append(f"catálogo de administradoras abaixo do piso: {len(admins)}")
    if len(products) == 0:
        errors.append("nenhuma operação observada em produtos")
    roots = [x.get("cnpj_root") for x in admins]
    if any(not isinstance(r, str) or len(r) != 8 or not r.isdigit() for r in roots):
        errors.append("administradora com cnpj_root inválido")
    if len(roots) != len(set(roots)):
        errors.append("cnpj_root duplicado no catálogo")

    if meta.get("methodology", {}).get("general_score") is not False:
        errors.append("meta permite score geral")
    if any("scores" in x for x in admins if isinstance(x, dict)):
        errors.append("score legado presente em administradoras")
    if any(x.get("comparabilidade", {}).get("ranking_geral_publicavel") is not False for x in admins):
        errors.append("ranking_geral_publicavel deve ser false para todas as administradoras")

    # A taxonomia válida vem da própria release, derivada da fonte oficial.
    taxonomy = meta.get("segment_taxonomy")
    valid_segments: set[str] = set()
    if not isinstance(taxonomy, dict):
        errors.append("meta.segment_taxonomy ausente/inválido")
    else:
        if taxonomy.get("contract") != TAXONOMY_CONTRACT:
            errors.append("meta.segment_taxonomy.contract inválido")
        if taxonomy.get("status") not in SAFE_TAXONOMY_STATUS:
            errors.append(f"meta.segment_taxonomy.status não publicável: {taxonomy.get('status')!r}")

        known_codes = taxonomy.get("known_codes")
        if not isinstance(known_codes, list) or not known_codes:
            errors.append("meta.segment_taxonomy.known_codes ausente/vazio")
        else:
            normalized_codes = [str(code) for code in known_codes]
            if any(not code.isdigit() for code in normalized_codes):
                errors.append("meta.segment_taxonomy.known_codes contém código não numérico")
            if len(normalized_codes) != len(set(normalized_codes)):
                errors.append("meta.segment_taxonomy.known_codes contém duplicatas")
            valid_segments = set(normalized_codes)

        observed_codes = taxonomy.get("observed_codes")
        if not isinstance(observed_codes, list) or not observed_codes:
            errors.append("meta.segment_taxonomy.observed_codes ausente/vazio")
        elif valid_segments:
            unknown_observed = sorted({str(x) for x in observed_codes} - valid_segments)
            if unknown_observed:
                errors.append(f"códigos observados fora da taxonomia oficial: {unknown_observed}")

        additions = taxonomy.get("auto_added_codes", [])
        if not isinstance(additions, list):
            errors.append("meta.segment_taxonomy.auto_added_codes inválido")
        elif valid_segments:
            invalid_additions = sorted({str(x) for x in additions} - valid_segments)
            if invalid_additions:
                errors.append(f"auto_added_codes fora da taxonomia: {invalid_additions}")

    segment_metadata = payloads["segmentos.json"].get("metadata", {})
    if isinstance(segment_metadata, dict):
        if segment_metadata.get("taxonomy_contract") != TAXONOMY_CONTRACT:
            errors.append("segmentos.metadata.taxonomy_contract inválido")
        declared_codes = segment_metadata.get("official_codes")
        if valid_segments and {str(x) for x in (declared_codes or [])} != valid_segments:
            errors.append("segmentos.metadata.official_codes diverge de meta.segment_taxonomy.known_codes")

    product_keys = []
    for product in products:
        segment = str(product.get("segmento_codigo") or "")
        if not valid_segments or segment not in valid_segments:
            errors.append(f"segmento inválido: {segment!r}")
        key = (product.get("cnpj_root"), product.get("competencia"), segment)
        product_keys.append(key)
        if not any((product.get(k) or 0) > 0 for k in (
            "taxa_administracao_pct",
            "grupos_ativos",
            "cotas_ativas_em_dia",
            "contemplacoes_mes",
            "inadimplentes",
        )):
            errors.append(f"produto sem sinal operacional positivo: {key}")
        if "segmentos_consolidados" not in str(product.get("source_member") or "").lower():
            errors.append(f"produto fora da fonte consolidada: {key}")
    if len(product_keys) != len(set(product_keys)):
        errors.append("chave cnpj_root+competencia+segmento duplicada em produtos")

    segment_codes = {str(x.get("codigo") or "") for x in segments if isinstance(x, dict)}
    if valid_segments and not segment_codes.issubset(valid_segments):
        errors.append(f"segmentos.json contém códigos fora da taxonomia: {sorted(segment_codes - valid_segments)}")
    comparison_keys = {str(x.get("segmento") or "") for x in comparisons if isinstance(x, dict)}
    segment_keys = {str(x.get("segmento") or "") for x in segments if isinstance(x, dict)}
    if comparison_keys != segment_keys:
        errors.append("comparacoes.json e segmentos.json divergem no conjunto de segmentos publicados")

    source_period = meta.get("source_periods", {}).get("consorciobd_mensal")
    product_periods = sorted({x.get("competencia") for x in products if x.get("competencia")})
    if product_periods and source_period != product_periods[-1]:
        errors.append(f"competência do meta ({source_period}) diverge de produtos ({product_periods[-1]})")

    declared = set()
    artifacts = meta.get("artifacts")
    if not isinstance(artifacts, dict):
        errors.append("meta.artifacts ausente/inválido")
    else:
        entries = artifacts.get("global")
        if not isinstance(entries, list):
            errors.append("meta.artifacts.global inválido")
        else:
            for entry in entries:
                if not isinstance(entry, dict) or not entry.get("file"):
                    errors.append("entrada de manifesto inválida em global")
                    continue
                filename = entry["file"]
                path = g / filename
                declared.add(("global", filename))
                if not path.exists():
                    errors.append(f"manifesto declara arquivo ausente: global/{filename}")
                    continue
                raw = path.read_bytes()
                if hashlib.sha256(raw).hexdigest() != entry.get("sha256"):
                    errors.append(f"sha256 divergente: global/{filename}")
                if len(raw) != entry.get("size_bytes"):
                    errors.append(f"size_bytes divergente: global/{filename}")

        seo_entries = artifacts.get("seo", [])
        if seo_entries not in (None, []):
            errors.append("release data-only não pode declarar meta.artifacts.seo")
        extra_families = sorted(set(artifacts) - {"global", "seo"})
        if extra_families:
            errors.append(f"famílias de artefatos não suportadas: {extra_families}")

    physical = {
        ("global", p.name) for p in g.glob("*.json") if p.name != "meta.json"
    }
    if legacy_seo.is_dir():
        physical.update(("seo", p.name) for p in legacy_seo.glob("*.json"))
    if physical != declared:
        errors.append(f"inventário físico != manifesto: physical={sorted(physical)} declared={sorted(declared)}")

    provenance_cfg = load(Path(args.provenance_config))
    required_sources = provenance_cfg.get("required_sources", {})
    if not isinstance(required_sources, dict) or not required_sources:
        errors.append("config de proveniência sem required_sources")
        required_sources = {}

    source_status = meta.get("source_status")
    if not isinstance(source_status, dict):
        errors.append("meta.source_status ausente")
        source_status = {}

    for source, spec in required_sources.items():
        state = source_status.get(source)
        if not isinstance(state, dict):
            errors.append(f"source_status ausente: {source}")
            continue
        if state.get("last_check_status") not in {"success", "failure"}:
            errors.append(f"source_status.{source}.last_check_status inválido")
        for field in ("last_checked_at", "last_successful_check_at"):
            if not state.get(field):
                errors.append(f"source_status.{source}.{field} ausente")
        content_hash = state.get("content_sha256")
        if not isinstance(content_hash, str) or re.fullmatch(r"[a-f0-9]{64}", content_hash) is None:
            errors.append(f"source_status.{source}.content_sha256 inválido")
        competence = state.get("competence")
        if not isinstance(competence, dict) or "kind" not in competence or "value" not in competence:
            errors.append(f"source_status.{source}.competence inválida")
            continue
        competence_mode = str(spec.get("competence_mode") or "auto") if isinstance(spec, dict) else "auto"
        if competence_mode == "not_applicable":
            if competence.get("kind") != "not_applicable":
                errors.append(f"source_status.{source}.competence deveria ser not_applicable")
        elif competence.get("kind") in {"unknown", "not_reported", "not_applicable"} or competence.get("value") in {None, ""}:
            errors.append(f"source_status.{source}.competence não resolvida")

    monthly_state = source_status.get("bc_consorciobd")
    if isinstance(monthly_state, dict):
        competence = monthly_state.get("competence")
        state_period = competence.get("value") if isinstance(competence, dict) else None
        if state_period != source_period:
            errors.append(f"competência ConsorcioBD diverge entre source_status ({state_period}) e meta ({source_period})")

    backend_release = meta.get("backend_release")
    if not isinstance(backend_release, dict) or backend_release.get("contract") != RELEASE_CONTRACT:
        errors.append("backend_release contract ausente/inválido")
    else:
        if backend_release.get("publication_eligible") is not True:
            errors.append("backend_release não elegível para publicação")
        release_fingerprint = backend_release.get("release_fingerprint")
        if not isinstance(release_fingerprint, str) or re.fullmatch(r"[a-f0-9]{64}", release_fingerprint) is None:
            errors.append("backend_release.release_fingerprint inválido")

    freshness = meta.get("freshness")
    if not isinstance(freshness, dict):
        errors.append("freshness ausente/inválido")
    else:
        if freshness.get("all_required_states_present") is not True:
            errors.append("freshness/all_required_states_present inválido")
        if freshness.get("source_state_matches_consumed_bytes") is not True:
            errors.append("freshness/source_state_matches_consumed_bytes inválido")
        degraded = freshness.get("degraded_sources")
        if not isinstance(degraded, list):
            errors.append("freshness.degraded_sources inválido")
        else:
            expected_degraded = sorted(
                source for source in required_sources
                if isinstance(source_status.get(source), dict)
                and source_status[source].get("last_check_status") != "success"
            )
            if sorted(degraded) != expected_degraded:
                errors.append(f"freshness.degraded_sources divergente: {sorted(degraded)} != {expected_degraded}")

    if isinstance(meta.get("counts"), dict):
        expected_counts = {
            "administradoras": len(admins),
            "produtos": len(products),
            "rankings": len(rankings),
            "segmentos": len(segments),
            "comparacoes": len(comparisons),
        }
        for key, value in expected_counts.items():
            if meta["counts"].get(key) != value:
                errors.append(f"meta.counts.{key} divergente: {meta['counts'].get(key)} != {value}")

    if args.deploy_config:
        deploy = load(Path(args.deploy_config))
        if deploy.get("artifact_source") != args.dist_base:
            errors.append(
                f"deploy artifact_source divergente: {deploy.get('artifact_source')!r} != {args.dist_base!r}"
            )
        if deploy.get("manifest") != "global/meta.json":
            errors.append("deploy manifest deve ser global/meta.json")
        if deploy.get("release_contract") != RELEASE_CONTRACT:
            errors.append("deploy release_contract divergente")
        deploy_artifacts = deploy.get("artifacts", {})
        if not isinstance(deploy_artifacts, dict) or "global" not in deploy_artifacts:
            errors.append("deploy artifacts.global ausente")
        if isinstance(deploy_artifacts, dict) and deploy_artifacts.get("seo") not in (None, []):
            errors.append("deploy V2 data-only não pode declarar artefatos SEO")

    if args.audit_baseline:
        baseline = load(Path(args.audit_baseline))
        expected = baseline.get("expected", {})
        if expected.get("administradoras") is not None and len(admins) != expected["administradoras"]:
            errors.append(f"baseline administradoras: {len(admins)} != {expected['administradoras']}")
        if expected.get("ranking_rows") is not None and len(rankings) != expected["ranking_rows"]:
            errors.append(f"baseline ranking_rows: {len(rankings)} != {expected['ranking_rows']}")
        if expected.get("products") is not None and len(products) != expected["products"]:
            errors.append(f"baseline products: {len(products)} != {expected['products']}")
        if expected.get("operational_roots") is not None:
            actual = len({x.get("cnpj_root") for x in products})
            if actual != expected["operational_roots"]:
                errors.append(f"baseline operational_roots: {actual} != {expected['operational_roots']}")
        if expected.get("segment_counts"):
            actual_counts = Counter(str(x.get("segmento_codigo")) for x in products)
            normalized = {str(k): int(v) for k, v in expected["segment_counts"].items()}
            if dict(actual_counts) != normalized:
                errors.append(f"baseline segment_counts: {dict(actual_counts)} != {normalized}")
        if expected.get("monthly_consolidated_rows") is not None and meta.get("quality", {}).get("monthly_consolidated_rows") != expected["monthly_consolidated_rows"]:
            errors.append(f"baseline monthly_consolidated_rows: {meta.get('quality', {}).get('monthly_consolidated_rows')} != {expected['monthly_consolidated_rows']}")
        if expected.get("monthly_operational_rows") is not None and meta.get("quality", {}).get("monthly_operational_rows") != expected["monthly_operational_rows"]:
            errors.append(f"baseline monthly_operational_rows: {meta.get('quality', {}).get('monthly_operational_rows')} != {expected['monthly_operational_rows']}")
        if expected.get("monthly_operational_roots") is not None and meta.get("quality", {}).get("monthly_operational_roots") != expected["monthly_operational_roots"]:
            errors.append(f"baseline monthly_operational_roots: {meta.get('quality', {}).get('monthly_operational_roots')} != {expected['monthly_operational_roots']}")
        if expected.get("all_positions_null") is True and any(x.get("posicao_oficial") is not None for x in rankings):
            errors.append("baseline exige posicao_oficial nula em todos os registros")
        if isinstance(expected.get("portfolios"), dict):
            by_root = {x.get("cnpj_root"): x for x in admins}
            for root_key, segment_codes_expected in expected["portfolios"].items():
                admin = by_root.get(root_key)
                if not admin:
                    errors.append(f"baseline administradora ausente para portfolio: {root_key}")
                    continue
                actual = [x.get("codigo") for x in admin.get("portfolio_observado", {}).get("segmentos", [])]
                if actual != segment_codes_expected:
                    errors.append(f"baseline portfolio {root_key}: {actual} != {segment_codes_expected}")
        if expected.get("orphan_operational_root"):
            orphan = expected["orphan_operational_root"]
            if orphan not in meta.get("quality", {}).get("operational_orphans", []):
                errors.append(f"baseline órfão operacional ausente: {orphan}")
            if orphan not in meta.get("quality", {}).get("ranking_orphans", []):
                errors.append(f"baseline órfão ranking ausente: {orphan}")
        imoveis_expected = expected.get("imoveis")
        if isinstance(imoveis_expected, dict):
            imoveis = [x for x in products if x.get("segmento_codigo") == "1"]
            fields = {
                "cotas_ativas_em_dia": "cotas_ativas_em_dia",
                "contemplacoes_mes": "contemplacoes_mes",
                "inadimplentes": "inadimplentes",
                "grupos_ativos": "grupos_ativos",
            }
            for baseline_key, field in fields.items():
                if baseline_key in imoveis_expected:
                    actual = sum(x.get(field) or 0 for x in imoveis)
                    if actual != imoveis_expected[baseline_key]:
                        errors.append(f"baseline imóveis {field}: {actual} != {imoveis_expected[baseline_key]}")

    fail(errors)
    print(json.dumps({
        "status": "PASS",
        "pipeline_version": meta.get("pipeline_version"),
        "release_contract": meta.get("backend_release", {}).get("contract"),
        "release_scope": meta.get("release_scope", {}).get("kind"),
        "segment_taxonomy_contract": meta.get("segment_taxonomy", {}).get("contract"),
        "segment_taxonomy_status": meta.get("segment_taxonomy", {}).get("status"),
        "segment_codes": meta.get("segment_taxonomy", {}).get("known_codes", []),
        "administradoras": len(admins),
        "produtos": len(products),
        "ranking_rows": len(rankings),
        "competencia": source_period,
        "degraded_sources": meta.get("freshness", {}).get("degraded_sources", []),
        "release_fingerprint": meta.get("backend_release", {}).get("release_fingerprint"),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
