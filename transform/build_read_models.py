#!/usr/bin/env python3
"""
Build dos read models do comparador de consórcios.

Gera:
- data/dist/global/instituicoes.json
- data/dist/global/produtos.json
- data/dist/global/rankings.json
- data/dist/global/series.json
- data/dist/global/cenarios.json
- data/dist/global/autocomplete.json
- data/dist/global/meta.json
- data/dist/seo/*.json
- data/runtime/build_read_models.json
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from unidecode import unidecode

PIPELINE_VERSION = "2.1.0"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def try_load_json(path: Path) -> Any:
    if not path.exists():
        return None
    return load_json(path)


def dump_json_text(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def write_text_if_changed(path: Path, text: str) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    previous = path.read_text(encoding="utf-8") if path.exists() else None
    if previous == text:
        return False
    path.write_text(text, encoding="utf-8")
    return True


def write_json_if_changed(path: Path, data: Any) -> bool:
    return write_text_if_changed(path, dump_json_text(data))


def append_github_output(name: str, value: str) -> None:
    github_output = os.environ.get("GITHUB_OUTPUT")
    if not github_output:
        return
    with open(github_output, "a", encoding="utf-8") as fh:
        fh.write(f"{name}={value}\n")


def sha256_text(text: str) -> str:
    import hashlib
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_bytes(data: bytes) -> str:
    import hashlib
    return hashlib.sha256(data).hexdigest()


def normalize_spaces(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    return text or None


def normalize_key(value: str) -> str:
    text = unidecode(str(value or "")).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def normalize_text(value: Optional[str]) -> str:
    return normalize_key(value or "")


def slugify(value: str) -> str:
    return normalize_key(value).replace("_", "-")


def try_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(".", "").replace(",", ".") if "," in text and "." in text else text.replace(",", ".")
    match = re.search(r"-?\d+(\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def try_int(value: Any) -> Optional[int]:
    number = try_float(value)
    if number is None:
        return None
    return int(number)


def extract_first_list_of_dicts(node: Any) -> List[Dict[str, Any]]:
    if isinstance(node, list) and node and all(isinstance(x, dict) for x in node):
        return node

    if isinstance(node, dict):
        for preferred_key in (
            "items",
            "rows",
            "records",
            "data",
            "instituicoes",
            "filiais",
            "resources",
            "series",
            "top_candidates",
            "probe_results",
        ):
            value = node.get(preferred_key)
            if isinstance(value, list) and value and all(isinstance(x, dict) for x in value):
                return value

        for value in node.values():
            found = extract_first_list_of_dicts(value)
            if found:
                return found

    if isinstance(node, list):
        for item in node:
            found = extract_first_list_of_dicts(item)
            if found:
                return found

    return []


def normalize_record_keys(record: Dict[str, Any]) -> Dict[str, Any]:
    return {normalize_key(str(k)): v for k, v in record.items()}


def pick_value(record: Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in record and record[key] not in (None, "", []):
            return record[key]
    return None


def detect_csv_delimiter(text: str) -> str:
    sample = text[:10000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
        return dialect.delimiter
    except Exception:
        return ";" if sample.count(";") > sample.count(",") else ","


def parse_csv_bytes(binary: bytes) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    text = None
    encoding_used = None
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            text = binary.decode(encoding)
            encoding_used = encoding
            break
        except Exception:
            continue

    if text is None:
        raise ValueError("Não foi possível decodificar bytes CSV.")

    delimiter = detect_csv_delimiter(text)
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    rows = []
    for row in reader:
        normalized = normalize_record_keys(row)
        rows.append(normalized)

    meta = {
        "encoding": encoding_used,
        "delimiter": delimiter,
        "row_count": len(rows),
        "headers": list(rows[0].keys()) if rows else [],
        "preview": " ".join(text.split())[:300],
    }
    return rows, meta


def summarize_zip_csv(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "exists": False,
            "members": [],
            "headers_union": [],
            "row_count_total": 0,
        }

    members_summary = []
    headers_union = set()
    row_count_total = 0

    with zipfile.ZipFile(path, "r") as zf:
        for member in zf.namelist():
            if not member.lower().endswith(".csv"):
                continue

            with zf.open(member, "r") as fh:
                binary = fh.read()

            rows, meta = parse_csv_bytes(binary)
            row_count_total += meta["row_count"]
            headers_union.update(meta["headers"])
            members_summary.append(
                {
                    "member": member,
                    "row_count": meta["row_count"],
                    "headers": meta["headers"][:40],
                }
            )

    return {
        "exists": True,
        "members": members_summary,
        "headers_union": sorted(headers_union),
        "row_count_total": row_count_total,
    }


def load_product_rules(path: Path) -> List[Dict[str, Any]]:
    payload = try_load_json(path)
    if payload is None:
        return fallback_product_rules()

    if isinstance(payload, dict):
        if isinstance(payload.get("products"), list):
            items = payload["products"]
        elif isinstance(payload.get("rules"), list):
            items = payload["rules"]
        else:
            items = []
            for key, value in payload.items():
                if isinstance(value, dict):
                    row = {"key": key, **value}
                    items.append(row)
    elif isinstance(payload, list):
        items = payload
    else:
        items = []

    normalized_items = []
    for item in items:
        if not isinstance(item, dict):
            continue
        key = item.get("key") or slugify(item.get("label") or item.get("name") or "produto")
        label = item.get("label") or item.get("name") or key.replace("-", " ").title()
        aliases = item.get("aliases") or item.get("keywords") or []
        intents = item.get("intents") or item.get("intent_keywords") or []
        normalized_items.append(
            {
                "key": normalize_key(str(key)),
                "label": str(label),
                "aliases": [str(x) for x in aliases],
                "intents": [str(x) for x in intents],
            }
        )

    return normalized_items or fallback_product_rules()


def fallback_product_rules() -> List[Dict[str, Any]]:
    return [
        {"key": "carro", "label": "Consórcio de Carro", "aliases": ["carro", "automovel", "automóvel", "veiculo"], "intents": ["consórcio de carro", "melhor consórcio de carro"]},
        {"key": "moto", "label": "Consórcio de Moto", "aliases": ["moto", "motocicleta"], "intents": ["consórcio de moto"]},
        {"key": "imobiliario", "label": "Consórcio Imobiliário", "aliases": ["imovel", "imóvel", "imobiliario", "imobiliário", "casa", "apartamento"], "intents": ["consórcio imobiliário"]},
        {"key": "servicos", "label": "Consórcio de Serviços", "aliases": ["servico", "serviço", "servicos", "serviços"], "intents": ["consórcio de serviços"]},
    ]


def load_seo_routes(path: Path) -> List[Dict[str, Any]]:
    payload = try_load_json(path)
    if payload is None:
        return fallback_seo_routes()

    if isinstance(payload, dict):
        if isinstance(payload.get("routes"), list):
            items = payload["routes"]
        elif isinstance(payload.get("seo_routes"), list):
            items = payload["seo_routes"]
        else:
            items = []
            for key, value in payload.items():
                if isinstance(value, dict):
                    items.append({"key": key, **value})
    elif isinstance(payload, list):
        items = payload
    else:
        items = []

    normalized_items = []
    for item in items:
        if not isinstance(item, dict):
            continue
        key = item.get("key") or item.get("slug") or item.get("file") or item.get("name")
        if not key:
            continue
        slug = str(item.get("slug") or item.get("file") or key)
        slug = slug.replace(".json", "").strip("/")
        title = item.get("title") or item.get("label") or slug.replace("-", " ").title()
        path_value = item.get("path") or f"/financas/consorcio/{slug}/"
        if slug in ("melhor-consorcio", "consorcio-ou-financiamento", "ranking-administradoras", "menor-taxa"):
            path_value = item.get("path") or f"/financas/consorcio/{slug}.php"

        normalized_items.append(
            {
                "key": normalize_key(str(key)),
                "slug": slug,
                "title": str(title),
                "path": str(path_value),
                "primary_keyword": str(item.get("primary_keyword") or item.get("keyword") or title),
            }
        )

    return normalized_items or fallback_seo_routes()


def fallback_seo_routes() -> List[Dict[str, Any]]:
    return [
        {"key": "home_consorcio", "slug": "home-consorcio", "title": "Comparador de Consórcios", "path": "/financas/consorcio/", "primary_keyword": "comparador de consórcios"},
        {"key": "carro", "slug": "carro", "title": "Consórcio de Carro", "path": "/financas/consorcio/carro/", "primary_keyword": "consórcio de carro"},
        {"key": "moto", "slug": "moto", "title": "Consórcio de Moto", "path": "/financas/consorcio/moto/", "primary_keyword": "consórcio de moto"},
        {"key": "imobiliario", "slug": "imobiliario", "title": "Consórcio Imobiliário", "path": "/financas/consorcio/imobiliario/", "primary_keyword": "consórcio imobiliário"},
        {"key": "carta_contemplada", "slug": "carta-contemplada", "title": "Carta Contemplada", "path": "/financas/consorcio/carta-contemplada/", "primary_keyword": "carta contemplada"},
        {"key": "melhor_consorcio", "slug": "melhor-consorcio", "title": "Melhor Consórcio", "path": "/financas/consorcio/melhor-consorcio.php", "primary_keyword": "melhor consórcio"},
        {"key": "consorcio_ou_financiamento", "slug": "consorcio-ou-financiamento", "title": "Consórcio ou Financiamento", "path": "/financas/consorcio/consorcio-ou-financiamento.php", "primary_keyword": "consórcio ou financiamento"},
        {"key": "ranking_administradoras", "slug": "ranking-administradoras", "title": "Ranking de Administradoras", "path": "/financas/consorcio/ranking-administradoras.php", "primary_keyword": "ranking de administradoras de consórcio"},
        {"key": "menor_taxa", "slug": "menor-taxa", "title": "Consórcio com Menor Taxa", "path": "/financas/consorcio/menor-taxa.php", "primary_keyword": "consórcio com menor taxa"},
    ]


def load_scoring_rules(path: Path) -> Dict[str, Any]:
    payload = try_load_json(path)
    if isinstance(payload, dict):
        return payload
    return {}


def load_stage_institutions(path: Path) -> List[Dict[str, Any]]:
    payload = try_load_json(path)
    if payload is None:
        return []

    records = extract_first_list_of_dicts(payload)
    if not records and isinstance(payload, list):
        records = payload

    institutions = []
    for record in records:
        row = normalize_record_keys(record)
        name = pick_value(row, ["institution_name", "nome", "name", "noenti", "razao_social", "nome_fantasia"])
        if not name:
            continue
        institutions.append(
            {
                "institution_id": pick_value(row, ["institution_id", "coenti", "codigo", "id"]),
                "cnpj": pick_value(row, ["cnpj", "cpf_cnpj", "cnpj14"]),
                "institution_name": str(name),
                "status": pick_value(row, ["status", "situacao", "st", "situacao_cadastral"]),
                "source_row": row,
                "normalized_name": normalize_text(str(name)),
            }
        )

    return institutions


def load_stage_filiais(path: Path) -> List[Dict[str, Any]]:
    payload = try_load_json(path)
    if payload is None:
        return []

    records = extract_first_list_of_dicts(payload)
    if not records and isinstance(payload, list):
        records = payload

    branches = []
    for record in records:
        row = normalize_record_keys(record)
        name = pick_value(row, ["institution_name", "nome", "administradora", "noenti", "razao_social"])
        if not name:
            continue
        branches.append(
            {
                "institution_name": str(name),
                "cnpj": pick_value(row, ["cnpj", "cpf_cnpj", "cnpj14"]),
                "uf": pick_value(row, ["uf", "sigla_uf"]),
                "cidade": pick_value(row, ["cidade", "municipio", "municipio_nome"]),
                "normalized_name": normalize_text(str(name)),
            }
        )

    return branches


def load_series_payload(path: Path) -> Dict[str, Any]:
    payload = try_load_json(path)
    if isinstance(payload, dict):
        return payload
    return {"metadata": {}, "series": []}


def extract_series_entries(node: Any) -> List[Dict[str, Any]]:
    found = []

    def walk(item: Any) -> None:
        if isinstance(item, dict):
            if "key" in item and ("label" in item or "values" in item or "latest_value" in item or "last_value" in item):
                found.append(item)
            for value in item.values():
                walk(value)
        elif isinstance(item, list):
            for value in item:
                walk(value)

    walk(node)
    return found


def extract_latest_series_map(series_payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    latest = {}
    for entry in extract_series_entries(series_payload):
        key = str(entry.get("key"))
        label = entry.get("label") or key
        value = entry.get("latest_value")
        date_value = entry.get("latest_date") or entry.get("last_date")

        if value is None:
            values = entry.get("values")
            if isinstance(values, list) and values:
                last = values[-1]
                if isinstance(last, dict):
                    value = last.get("value") or last.get("valor")
                    date_value = date_value or last.get("date") or last.get("data")

        latest[key] = {
            "key": key,
            "label": label,
            "value": try_float(value) if value is not None else value,
            "date": date_value,
            "unit": entry.get("unit"),
            "frequency": entry.get("frequency"),
        }
    return latest


def load_ranking_rows(csv_path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not csv_path.exists():
        return [], {"exists": False}

    binary = csv_path.read_bytes()
    rows, meta = parse_csv_bytes(binary)

    parsed = []
    for row in rows:
        name = pick_value(
            row,
            [
                "instituicao",
                "nome",
                "administradora",
                "instituicao_financeira",
                "nome_instituicao",
                "razao_social",
            ],
        )
        if not name:
            continue

        parsed.append(
            {
                "institution_name": str(name),
                "normalized_name": normalize_text(str(name)),
                "position": try_int(pick_value(row, ["posicao", "ranking", "classificacao"])),
                "index_value": try_float(pick_value(row, ["indice", "indice_reclamacoes", "indice_de_reclamacoes"])),
                "complaints": try_int(pick_value(row, ["reclamacoes_reguladas_procedentes", "reclamacoes", "qtde_reclamacoes"])),
                "clients": try_int(pick_value(row, ["clientes", "quantidade_clientes", "base_clientes"])),
                "raw": row,
            }
        )

    parsed.sort(key=lambda x: (x["position"] is None, x["position"] or 999999))
    meta["exists"] = True
    return parsed, meta


def build_branch_counts(branches: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    by_name: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"branch_count": 0, "ufs": set(), "cities": set()})
    for row in branches:
        key = row["normalized_name"]
        by_name[key]["branch_count"] += 1
        if row.get("uf"):
            by_name[key]["ufs"].add(str(row["uf"]))
        if row.get("cidade"):
            by_name[key]["cities"].add(str(row["cidade"]))

    result = {}
    for key, value in by_name.items():
        result[key] = {
            "branch_count": value["branch_count"],
            "ufs": sorted(value["ufs"]),
            "cities_sample": sorted(value["cities"])[:15],
        }
    return result


def build_ranking_lookup(rankings: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lookup = {}
    for row in rankings:
        key = row["normalized_name"]
        if key and key not in lookup:
            lookup[key] = row
    return lookup


def compute_institution_score(
    *,
    status: Optional[str],
    branch_count: int,
    ranking_position: Optional[int],
    scoring_rules: Dict[str, Any],
) -> float:
    defaults = {
        "base_score": 50,
        "active_bonus": 12,
        "branch_weight": 1.2,
        "ranking_weight": 14,
        "ranking_max_position": 20,
        "branch_cap": 20,
    }
    rules = {**defaults, **(scoring_rules.get("instituicoes", {}) if isinstance(scoring_rules.get("instituicoes"), dict) else {})}

    score = float(rules["base_score"])
    status_text = normalize_text(status)

    if any(term in status_text for term in ("ativo", "autorizado", "funcionamento")):
        score += float(rules["active_bonus"])

    score += min(branch_count * float(rules["branch_weight"]), float(rules["branch_cap"]))

    if ranking_position and ranking_position > 0:
        max_pos = int(rules["ranking_max_position"])
        if ranking_position <= max_pos:
            bonus = (max_pos - ranking_position + 1) / max_pos * float(rules["ranking_weight"])
            score += bonus

    return round(score, 2)


def build_instituicoes(
    cadastro: List[Dict[str, Any]],
    branches: List[Dict[str, Any]],
    rankings: List[Dict[str, Any]],
    scoring_rules: Dict[str, Any],
) -> Dict[str, Any]:
    branch_counts = build_branch_counts(branches)
    ranking_lookup = build_ranking_lookup(rankings)

    items = []
    for inst in cadastro:
        key = inst["normalized_name"]
        branch_info = branch_counts.get(key, {})
        ranking_info = ranking_lookup.get(key, {})
        score = compute_institution_score(
            status=inst.get("status"),
            branch_count=branch_info.get("branch_count", 0),
            ranking_position=ranking_info.get("position"),
            scoring_rules=scoring_rules,
        )

        items.append(
            {
                "institution_id": inst.get("institution_id"),
                "cnpj": inst.get("cnpj"),
                "institution_name": inst["institution_name"],
                "normalized_name": key,
                "status": inst.get("status"),
                "branch_count": branch_info.get("branch_count", 0),
                "ufs": branch_info.get("ufs", []),
                "cities_sample": branch_info.get("cities_sample", []),
                "ranking_position": ranking_info.get("position"),
                "ranking_index": ranking_info.get("index_value"),
                "ranking_complaints": ranking_info.get("complaints"),
                "score": score,
            }
        )

    items.sort(key=lambda x: (-x["score"], x["institution_name"]))

    return {
        "metadata": {
            "generated_at": utc_now_iso(),
            "count": len(items),
        },
        "items": items,
    }


def build_rankings(rankings: List[Dict[str, Any]], ranking_meta: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "metadata": {
            "generated_at": utc_now_iso(),
            "count": len(rankings),
            "source_meta": ranking_meta,
        },
        "items": rankings,
    }


def build_products(
    product_rules: List[Dict[str, Any]],
    seo_routes: List[Dict[str, Any]],
    monthly_summary: Dict[str, Any],
    uf_summary: Dict[str, Any],
    adm_summary: Dict[str, Any],
) -> Dict[str, Any]:
    route_slugs = [route["slug"] for route in seo_routes]
    source_headers = " ".join(monthly_summary.get("headers_union", []) + uf_summary.get("headers_union", []) + adm_summary.get("headers_union", []))
    source_headers_norm = normalize_text(source_headers)

    items = []
    for product in product_rules:
        aliases = [normalize_text(product["label"])] + [normalize_text(x) for x in product.get("aliases", [])]
        matched_aliases = [alias for alias in aliases if alias and alias in source_headers_norm]

        related_routes = []
        for slug in route_slugs:
            slug_norm = normalize_text(slug)
            if product["key"] in slug_norm or any(alias and alias in slug_norm for alias in aliases):
                related_routes.append(slug)

        items.append(
            {
                "key": product["key"],
                "label": product["label"],
                "aliases": product.get("aliases", []),
                "intents": product.get("intents", []),
                "matched_aliases_in_sources": matched_aliases,
                "related_routes": related_routes,
                "source_coverage": {
                    "monthly_consolidated": monthly_summary.get("exists", False),
                    "quarterly_uf": uf_summary.get("exists", False),
                    "quarterly_adm": adm_summary.get("exists", False),
                },
            }
        )

    return {
        "metadata": {
            "generated_at": utc_now_iso(),
            "count": len(items),
            "monthly_summary": monthly_summary,
            "uf_summary": uf_summary,
            "adm_summary": adm_summary,
        },
        "items": items,
    }


def build_cenarios(
    latest_series: Dict[str, Dict[str, Any]],
    instituicoes_payload: Dict[str, Any],
    rankings_payload: Dict[str, Any],
    abac_payload: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    rankings = rankings_payload.get("items", [])

    def series_value(key: str) -> Optional[float]:
        entry = latest_series.get(key)
        return entry.get("value") if entry else None

    selic = series_value("selic_meta")
    veic = series_value("taxa_media_financiamento_veiculos_pf")
    imob = series_value("taxa_media_credito_imobiliario_pf")
    credito = series_value("taxa_media_credito_livre_pf")

    top_ranked = rankings[:5]
    top_names = [item["institution_name"] for item in top_ranked if item.get("institution_name")]

    abac_period = None
    if isinstance(abac_payload, dict):
        selected = abac_payload.get("selected") or {}
        abac_period = selected.get("period_label")

    items = []

    if veic is not None and selic is not None:
        items.append(
            {
                "key": "consorcio_ou_financiamento_veiculos",
                "headline": "Veículos: comparar consórcio com financiamento segue essencial",
                "description": f"Taxa média observada no financiamento de veículos PF: {veic:.2f}. Meta Selic: {selic:.2f}. Em ambiente de crédito pressionado, o comparativo precisa olhar custo total e prazo de contemplação.",
                "evidence": {"taxa_financiamento_veiculos_pf": veic, "selic_meta": selic},
            }
        )

    if imob is not None:
        items.append(
            {
                "key": "imobiliario_contexto",
                "headline": "Imobiliário: a taxa de crédito continua sendo referência prática",
                "description": f"A taxa média de crédito imobiliário PF observada foi {imob:.2f}. Isso ajuda a contextualizar buscas por consórcio imobiliário e cenários de custo de oportunidade.",
                "evidence": {"taxa_credito_imobiliario_pf": imob},
            }
        )

    if credito is not None:
        items.append(
            {
                "key": "credito_livre_contexto",
                "headline": "Crédito livre PF segue como baliza macro para decisão",
                "description": f"A taxa média de crédito livre PF observada foi {credito:.2f}. O usuário tende a comparar consórcio com alternativas de crédito mesmo quando a busca começa por produto.",
                "evidence": {"taxa_credito_livre_pf": credito},
            }
        )

    if top_names:
        items.append(
            {
                "key": "ranking_reclamacoes_contexto",
                "headline": "Ranking regulatório ajuda a priorizar administradoras",
                "description": "O ranking oficial de reclamações entrou no build e já pode alimentar recortes por reputação regulatória.",
                "evidence": {"top_ranked_institutions": top_names},
            }
        )

    if abac_period:
        items.append(
            {
                "key": "abac_contexto_setorial",
                "headline": "Boletim setorial da ABAC disponível como apoio interpretativo",
                "description": f"O boletim mais recente identificado foi {abac_period}. Ele entra como contexto complementar, sem substituir a base oficial do BCB.",
                "evidence": {"abac_period": abac_period},
            }
        )

    return {
        "metadata": {
            "generated_at": utc_now_iso(),
            "count": len(items),
        },
        "items": items,
    }


def build_autocomplete(
    instituicoes_payload: Dict[str, Any],
    produtos_payload: Dict[str, Any],
    seo_routes: List[Dict[str, Any]],
) -> Dict[str, Any]:
    suggestions = []
    seen = set()

    for item in instituicoes_payload.get("items", [])[:150]:
        label = item["institution_name"]
        key = ("instituicao", normalize_text(label))
        if key in seen:
            continue
        seen.add(key)
        suggestions.append(
            {
                "type": "instituicao",
                "label": label,
                "target": "/financas/consorcio/ranking-administradoras.php",
            }
        )

    for item in produtos_payload.get("items", []):
        label = item["label"]
        key = ("produto", normalize_text(label))
        if key in seen:
            continue
        seen.add(key)
        target = None
        related_routes = item.get("related_routes") or []
        if related_routes:
            slug = related_routes[0]
            target = f"/financas/consorcio/{slug}/" if slug not in ("melhor-consorcio", "consorcio-ou-financiamento", "ranking-administradoras", "menor-taxa") else f"/financas/consorcio/{slug}.php"
        suggestions.append(
            {
                "type": "produto",
                "label": label,
                "target": target or "/financas/consorcio/",
            }
        )

    for route in seo_routes:
        key = ("rota", normalize_text(route["title"]))
        if key in seen:
            continue
        seen.add(key)
        suggestions.append(
            {
                "type": "rota",
                "label": route["title"],
                "target": route["path"],
            }
        )

    return {
        "metadata": {
            "generated_at": utc_now_iso(),
            "count": len(suggestions),
        },
        "items": suggestions,
    }


def get_build_context() -> Dict[str, Any]:
    return {
        "pipeline_version": PIPELINE_VERSION,
        "build_timestamp": utc_now_iso(),
        "git_sha": os.environ.get("GITHUB_SHA"),
        "git_ref": os.environ.get("GITHUB_REF"),
        "github_repository": os.environ.get("GITHUB_REPOSITORY"),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
    }


def build_freshness(runtime_sources: Dict[str, Any]) -> Dict[str, Any]:
    latest_checked = None
    latest_changed = None
    changed_sources = 0

    for item in runtime_sources.values():
        checked = item.get("last_checked_at")
        changed = item.get("last_changed_at")
        if checked and (latest_checked is None or checked > latest_checked):
            latest_checked = checked
        if changed and (latest_changed is None or changed > latest_changed):
            latest_changed = changed
        if item.get("changed") is True:
            changed_sources += 1

    return {
        "latest_source_check_at": latest_checked,
        "latest_source_change_at": latest_changed,
        "changed_sources_count": changed_sources,
        "runtime_sources_count": len(runtime_sources),
    }


def build_artifact_manifest(
    global_payloads: Dict[str, Any],
    seo_payloads: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    manifest = {"global": [], "seo": []}

    for filename, payload in global_payloads.items():
        text = dump_json_text(payload)
        manifest["global"].append(
            {
                "file": filename,
                "sha256": sha256_text(text),
                "size_bytes": len(text.encode("utf-8")),
            }
        )

    for slug, payload in seo_payloads.items():
        filename = f"{slug}.json"
        text = dump_json_text(payload)
        manifest["seo"].append(
            {
                "file": filename,
                "sha256": sha256_text(text),
                "size_bytes": len(text.encode("utf-8")),
            }
        )

    return manifest


def build_meta(
    *,
    build_context: Dict[str, Any],
    instituicoes_payload: Dict[str, Any],
    produtos_payload: Dict[str, Any],
    rankings_payload: Dict[str, Any],
    series_payload: Dict[str, Any],
    cenarios_payload: Dict[str, Any],
    autocomplete_payload: Dict[str, Any],
    seo_payloads: Dict[str, Any],
    runtime_sources: Dict[str, Any],
    artifact_manifest: Dict[str, Any],
) -> Dict[str, Any]:
    freshness = build_freshness(runtime_sources)

    return {
        "build_timestamp": build_context["build_timestamp"],
        "pipeline_version": build_context["pipeline_version"],
        "project": {
            "name": "comparador-consorcios-data",
            "generated_at": build_context["build_timestamp"],
            "git_sha": build_context.get("git_sha"),
            "git_ref": build_context.get("git_ref"),
            "github_repository": build_context.get("github_repository"),
            "run_id": build_context.get("run_id"),
            "run_attempt": build_context.get("run_attempt"),
        },
        "counts": {
            "instituicoes": len(instituicoes_payload.get("items", [])),
            "produtos": len(produtos_payload.get("items", [])),
            "rankings": len(rankings_payload.get("items", [])),
            "cenarios": len(cenarios_payload.get("items", [])),
            "autocomplete": len(autocomplete_payload.get("items", [])),
            "seo_routes": len(seo_payloads),
        },
        "source_runtime": runtime_sources,
        "freshness": freshness,
        "artifacts": artifact_manifest,
        "hashes": {
            "instituicoes": sha256_text(dump_json_text(instituicoes_payload)),
            "produtos": sha256_text(dump_json_text(produtos_payload)),
            "rankings": sha256_text(dump_json_text(rankings_payload)),
            "series": sha256_text(dump_json_text(series_payload)),
            "cenarios": sha256_text(dump_json_text(cenarios_payload)),
            "autocomplete": sha256_text(dump_json_text(autocomplete_payload)),
        },
    }


def build_seo_payloads(
    seo_routes: List[Dict[str, Any]],
    instituicoes_payload: Dict[str, Any],
    produtos_payload: Dict[str, Any],
    rankings_payload: Dict[str, Any],
    latest_series: Dict[str, Dict[str, Any]],
    cenarios_payload: Dict[str, Any],
    meta_payload: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    institutions = instituicoes_payload.get("items", [])
    products = produtos_payload.get("items", [])
    rankings = rankings_payload.get("items", [])
    cenarios = cenarios_payload.get("items", [])

    payloads = {}
    for route in seo_routes:
        slug = route["slug"]
        slug_norm = normalize_text(slug)

        related_products = []
        for product in products:
            if product["key"] in slug_norm or any(normalize_text(alias) in slug_norm for alias in product.get("aliases", [])):
                related_products.append(product)

        if not related_products and slug in ("carro", "moto", "imobiliario"):
            related_products = [p for p in products if p["key"] == slug]

        route_payload = {
            "route": route,
            "snapshot": {
                "instituicoes_count": len(institutions),
                "ranking_count": len(rankings),
                "products_count": len(products),
            },
            "top_instituicoes": institutions[:10],
            "top_rankings": rankings[:10],
            "related_products": related_products,
            "series_highlights": list(latest_series.values())[:10],
            "scenario_highlights": cenarios[:5],
            "meta_hash": meta_payload["hashes"]["instituicoes"],
        }
        payloads[slug] = route_payload

    return payloads


def load_runtime_summary(runtime_dir: Path) -> Dict[str, Any]:
    summary = {}
    if not runtime_dir.exists():
        return summary

    for path in sorted(runtime_dir.glob("*.json")):
        payload = try_load_json(path)
        if isinstance(payload, dict):
            summary[path.stem] = {
                "last_checked_at": payload.get("last_checked_at"),
                "last_changed_at": payload.get("last_changed_at"),
                "changed": payload.get("changed"),
                "mode_used": payload.get("mode_used"),
            }
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Build dos read models do comparador de consórcios.")
    parser.add_argument("--config", required=True, help="Caminho para config/sources.json")
    parser.add_argument("--scoring", required=True, help="Caminho para config/scoring_rules.json")
    parser.add_argument("--product-rules", required=True, help="Caminho para config/product_rules.json")
    parser.add_argument("--seo-routes", required=True, help="Caminho para config/seo_routes.json")
    args = parser.parse_args()

    config = load_json(Path(args.config))
    defaults = config.get("defaults", {})

    raw_base = Path(defaults.get("raw_base_dir", "data/raw"))
    stage_base = Path(defaults.get("stage_base_dir", "data/stage"))
    dist_base = Path(defaults.get("dist_base_dir", "data/dist"))
    runtime_dir = Path("data/runtime")

    dist_global_dir = dist_base / "global"
    dist_seo_dir = dist_base / "seo"
    runtime_file = runtime_dir / "build_read_models.json"

    dist_global_dir.mkdir(parents=True, exist_ok=True)
    dist_seo_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    scoring_rules = load_scoring_rules(Path(args.scoring))
    product_rules = load_product_rules(Path(args.product_rules))
    seo_routes = load_seo_routes(Path(args.seo_routes))
    build_context = get_build_context()

    cadastro_stage = stage_base / "cadastro" / "instituicoes_cadastro.json"
    filiais_stage = stage_base / "filiais" / "filiais.json"
    series_stage = stage_base / "series" / "series.json"
    consorciobd_monthly_raw = raw_base / "bc" / "consorciobd" / "latest_source.bin"
    consorciobd_uf_raw = raw_base / "bc" / "consorciobd_trimestral" / "latest_uf.bin"
    consorciobd_adm_raw = raw_base / "bc" / "consorciobd_trimestral" / "latest_adm.bin"
    ranking_raw_csv = raw_base / "bc" / "ranking_reclamacoes" / "latest_source.csv"
    abac_stage = stage_base / "abac" / "boletim" / "abac_boletim.json"

    missing_required = []
    for required_path in (cadastro_stage, filiais_stage, ranking_raw_csv):
        if not required_path.exists():
            missing_required.append(str(required_path))

    if missing_required:
        message = f"Build sem insumos obrigatórios: {', '.join(missing_required)}"
        runtime_payload = {
            "last_checked_at": utc_now_iso(),
            "changed": False,
            "mode_used": "build-read-models-failed",
            "pipeline_version": PIPELINE_VERSION,
            "error": message,
        }
        write_json_if_changed(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("mode_used", "build-read-models-failed")
        print(message, file=sys.stderr)
        return 1

    cadastro = load_stage_institutions(cadastro_stage)
    branches = load_stage_filiais(filiais_stage)
    series_payload = load_series_payload(series_stage)
    latest_series = extract_latest_series_map(series_payload)
    rankings, ranking_meta = load_ranking_rows(ranking_raw_csv)
    abac_payload = try_load_json(abac_stage)

    monthly_summary = summarize_zip_csv(consorciobd_monthly_raw)
    uf_summary = summarize_zip_csv(consorciobd_uf_raw)
    adm_summary = summarize_zip_csv(consorciobd_adm_raw)

    instituicoes_payload = build_instituicoes(cadastro, branches, rankings, scoring_rules)
    rankings_payload = build_rankings(rankings, ranking_meta)
    produtos_payload = build_products(product_rules, seo_routes, monthly_summary, uf_summary, adm_summary)
    cenarios_payload = build_cenarios(latest_series, instituicoes_payload, rankings_payload, abac_payload)
    autocomplete_payload = build_autocomplete(instituicoes_payload, produtos_payload, seo_routes)

    runtime_sources = load_runtime_summary(runtime_dir)

    seo_payloads = build_seo_payloads(
        seo_routes=seo_routes,
        instituicoes_payload=instituicoes_payload,
        produtos_payload=produtos_payload,
        rankings_payload=rankings_payload,
        latest_series=latest_series,
        cenarios_payload=cenarios_payload,
        meta_payload={"hashes": {"instituicoes": sha256_text(dump_json_text(instituicoes_payload))}},
    )

    artifact_manifest = build_artifact_manifest(
        global_payloads={
            "instituicoes.json": instituicoes_payload,
            "produtos.json": produtos_payload,
            "rankings.json": rankings_payload,
            "series.json": series_payload,
            "cenarios.json": cenarios_payload,
            "autocomplete.json": autocomplete_payload,
        },
        seo_payloads=seo_payloads,
    )

    meta_payload = build_meta(
        build_context=build_context,
        instituicoes_payload=instituicoes_payload,
        produtos_payload=produtos_payload,
        rankings_payload=rankings_payload,
        series_payload=series_payload,
        cenarios_payload=cenarios_payload,
        autocomplete_payload=autocomplete_payload,
        seo_payloads=seo_payloads,
        runtime_sources=runtime_sources,
        artifact_manifest=artifact_manifest,
    )

    changed = False
    changed |= write_json_if_changed(dist_global_dir / "instituicoes.json", instituicoes_payload)
    changed |= write_json_if_changed(dist_global_dir / "produtos.json", produtos_payload)
    changed |= write_json_if_changed(dist_global_dir / "rankings.json", rankings_payload)
    changed |= write_json_if_changed(dist_global_dir / "series.json", series_payload)
    changed |= write_json_if_changed(dist_global_dir / "cenarios.json", cenarios_payload)
    changed |= write_json_if_changed(dist_global_dir / "autocomplete.json", autocomplete_payload)
    changed |= write_json_if_changed(dist_global_dir / "meta.json", meta_payload)

    for slug, payload in seo_payloads.items():
        changed |= write_json_if_changed(dist_seo_dir / f"{slug}.json", payload)

    runtime_payload = {
        "last_checked_at": utc_now_iso(),
        "last_changed_at": utc_now_iso() if changed else None,
        "changed": changed,
        "mode_used": "build-read-models",
        "pipeline_version": PIPELINE_VERSION,
        "counts": {
            "instituicoes": len(instituicoes_payload.get("items", [])),
            "produtos": len(produtos_payload.get("items", [])),
            "rankings": len(rankings_payload.get("items", [])),
            "seo_routes": len(seo_payloads),
        },
        "outputs": {
            "global_dir": str(dist_global_dir),
            "seo_dir": str(dist_seo_dir),
            "meta_file": str(dist_global_dir / "meta.json"),
        },
    }
    write_json_if_changed(runtime_file, runtime_payload)

    append_github_output("changed", "true" if changed else "false")
    append_github_output("records", str(len(seo_payloads)))
    append_github_output("stage_hash", sha256_text(dump_json_text(meta_payload)))
    append_github_output("mode_used", "build-read-models")

    print(
        json.dumps(
            {
                "changed": changed,
                "records": len(seo_payloads),
                "mode_used": "build-read-models",
                "global_dir": str(dist_global_dir),
                "seo_dir": str(dist_seo_dir),
                "runtime_file": str(runtime_file),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
