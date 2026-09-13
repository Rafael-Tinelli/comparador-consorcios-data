#!/usr/bin/env python3
"""Validate a generated Comparador de Consórcios V2 release.

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
SEO_CONTRACTS = {
    "defaults.json": "seo.defaults.v2",
    "routes.json": "seo.routes.v2",
    "site.json": "seo.site.v2",
}


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
    parser = argparse.ArgumentParser(description="Valida release V2 gerada")
    parser.add_argument("--dist-base", required=True)
    parser.add_argument("--provenance-config", required=True)
    parser.add_argument("--deploy-config")
    parser.add_argument("--audit-baseline")
    args = parser.parse_args()

    root = Path(args.dist_base)
    g = root / "global"
    s = root / "seo"
    errors: List[str] = []

    meta_path = g / "meta.json"
    if not meta_path.exists():
        fail([f"meta ausente: {meta_path}"])
    meta = load(meta_path)

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

    for filename, expected in SEO_CONTRACTS.items():
        path = s / filename
        if not path.exists():
            errors.append(f"artefato SEO ausente: {filename}")
            continue
        payload = load(path)
        if contract_of(payload) != expected:
            errors.append(f"contrato SEO divergente em {filename}: {contract_of(payload)!r} != {expected!r}")

    fail(errors)
    errors = []

    admins = payloads["administradoras.json"].get("items", [])
    products = payloads["produtos.json"].get("items", [])
    rankings = payloads["rankings.json"].get("items", [])
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

    valid_segments = {"1", "2", "3", "4", "5", "6"}
    product_keys = []
    for product in products:
        segment = product.get("segmento_codigo")
        if segment not in valid_segments:
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

    source_period = meta.get("source_periods", {}).get("consorciobd_mensal")
    product_periods = sorted({x.get("competencia") for x in products if x.get("competencia")})
    if product_periods and source_period != product_periods[-1]:
        errors.append(f"competência do meta ({source_period}) diverge de produtos ({product_periods[-1]})")

    declared = set()
    artifacts = meta.get("artifacts")
    if not isinstance(artifacts, dict):
        errors.append("meta.artifacts ausente/inválido")
    else:
        for family, directory in (("global", g), ("seo", s)):
            entries = artifacts.get(family)
            if not isinstance(entries, list):
                errors.append(f"meta.artifacts.{family} inválido")
                continue
            for entry in entries:
                if not isinstance(entry, dict) or not entry.get("file"):
                    errors.append(f"entrada de manifesto inválida em {family}")
                    continue
                filename = entry["file"]
                path = directory / filename
                declared.add((family, filename))
                if not path.exists():
                    errors.append(f"manifesto declara arquivo ausente: {family}/{filename}")
                    continue
                raw = path.read_bytes()
                if hashlib.sha256(raw).hexdigest() != entry.get("sha256"):
                    errors.append(f"sha256 divergente: {family}/{filename}")
                if len(raw) != entry.get("size_bytes"):
                    errors.append(f"size_bytes divergente: {family}/{filename}")

    physical = {
        *(("global", p.name) for p in g.glob("*.json") if p.name != "meta.json"),
        *(("seo", p.name) for p in s.glob("*.json")),
    }
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
    if not isinstance(backend_release, dict) or backend_release.get("contract") != "comparador-v2-release.v1":
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
        if deploy.get("release_contract") != "comparador-v2-release.v1":
            errors.append("deploy release_contract divergente")

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
            for root, segment_codes in expected["portfolios"].items():
                admin = by_root.get(root)
                if not admin:
                    errors.append(f"baseline administradora ausente para portfolio: {root}")
                    continue
                actual = [x.get("codigo") for x in admin.get("portfolio_observado", {}).get("segmentos", [])]
                if actual != segment_codes:
                    errors.append(f"baseline portfolio {root}: {actual} != {segment_codes}")
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
