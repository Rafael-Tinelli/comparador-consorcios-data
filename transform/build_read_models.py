#!/usr/bin/env python3
"""
Build dos read models do comparador de consórcios.

Gera:
- data/dist/global/instituicoes.json
- data/dist/global/administradoras.json
- data/dist/global/segmentos.json
- data/dist/global/ofertas.json
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

PIPELINE_VERSION = "3.0.0"


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
    """
    Carrega regras semânticas de produtos.

    Importante: este arquivo é apoio de SEO/intenção. Ele NÃO é o read model
    comercial do widget. O read model real de produtos é gerado em build_products()
    a partir do ConsorcioBD.
    """
    payload = try_load_json(path)
    if payload is None:
        return fallback_product_rules()

    if isinstance(payload, dict):
        if isinstance(payload.get("products"), list):
            items = payload["products"]
        elif isinstance(payload.get("rules"), list):
            items = payload["rules"]
        elif isinstance(payload.get("items"), list):
            items = payload["items"]
        else:
            items = []
            for key, value in payload.items():
                if not isinstance(value, dict):
                    continue
                if not any(k in value for k in ("label", "name", "aliases", "keywords", "intents", "intent_keywords")):
                    continue
                items.append({"key": key, **value})
    elif isinstance(payload, list):
        items = payload
    else:
        items = []

    normalized_items = []
    seen = set()

    for item in items:
        if not isinstance(item, dict):
            continue

        key = item.get("key") or slugify(item.get("label") or item.get("name") or "produto")
        key = normalize_key(str(key))

        if not key or key in seen:
            continue

        seen.add(key)

        label = item.get("label") or item.get("name") or key.replace("_", " ").title()
        aliases = item.get("aliases") or item.get("keywords") or []
        intents = item.get("intents") or item.get("intent_keywords") or []

        if isinstance(aliases, str):
            aliases = [aliases]
        if isinstance(intents, str):
            intents = [intents]

        normalized_items.append(
            {
                "key": key,
                "label": str(label),
                "aliases": [str(x) for x in aliases if str(x).strip()],
                "intents": [str(x) for x in intents if str(x).strip()],
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
    """
    Carrega rotas SEO.

    Se o arquivo estiver em formato técnico/configuração, NÃO transforma chaves como
    defaults/routes/families em páginas. Nesse caso usa fallback estável.
    """
    payload = try_load_json(path)
    if payload is None:
        return fallback_seo_routes()

    if isinstance(payload, dict):
        if isinstance(payload.get("routes"), list):
            items = payload["routes"]
        elif isinstance(payload.get("seo_routes"), list):
            items = payload["seo_routes"]
        elif isinstance(payload.get("items"), list):
            items = payload["items"]
        elif isinstance(payload.get("pages"), list):
            items = payload["pages"]
        else:
            items = []
            for key, value in payload.items():
                if not isinstance(value, dict):
                    continue
                if not any(k in value for k in ("path", "slug", "title", "primary_keyword", "keyword")):
                    continue
                items.append({"key": key, **value})
    elif isinstance(payload, list):
        items = payload
    else:
        items = []

    blocked_slugs = {
        "defaults", "routes", "families", "segment-aliases", "subsegment-aliases",
        "field-mapping", "normalization", "publication-rules", "comparison-rules",
        "sorting-defaults", "ui-filters", "route-visibility", "flags",
    }

    normalized_items = []
    seen_paths = set()
    seen_slugs = set()

    for item in items:
        if not isinstance(item, dict):
            continue

        key = item.get("key") or item.get("slug") or item.get("file") or item.get("name")
        if not key:
            continue

        raw_slug = str(item.get("slug") or item.get("file") or key)
        slug = slugify(raw_slug.replace(".json", "").strip("/"))

        if slug in blocked_slugs:
            continue

        title = item.get("title") or item.get("label") or slug.replace("-", " ").title()
        path_value = str(item.get("path") or f"/financas/consorcio/{slug}/")

        if slug in ("melhor-consorcio", "consorcio-ou-financiamento", "ranking-administradoras", "menor-taxa"):
            path_value = str(item.get("path") or f"/financas/consorcio/{slug}.php")

        if slug in seen_slugs or path_value in seen_paths:
            continue

        seen_slugs.add(slug)
        seen_paths.add(path_value)

        normalized_items.append(
            {
                "key": normalize_key(str(key)),
                "slug": slug,
                "title": str(title),
                "path": path_value,
                "primary_keyword": str(item.get("primary_keyword") or item.get("keyword") or title),
                "description": item.get("description") or item.get("meta_description"),
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
    """
    Lê o ranking de reclamações de consórcios do Banco Central.

    O CSV real vem com colunas como:
    - Ano
    - Semestre
    - CNPJ AC
    - Administradora de consórcio
    - Índice
    - Quantidade total de reclamações
    - Quantidade de clientes – Consorciados
    """
    if not csv_path.exists():
        return [], {"exists": False}

    binary = csv_path.read_bytes()
    rows, meta = parse_csv_bytes(binary)

    parsed = []
    for idx, row in enumerate(rows, start=1):
        name = pick_value(
            row,
            [
                "administradora_de_consorcio",
                "nome_da_administradora",
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

        cnpj_root = pick_value(row, ["cnpj_ac", "cnpj", "cnpj_da_administradora"])
        cnpj_root = re.sub(r"\D+", "", str(cnpj_root or "")) or None

        index_value = try_float(pick_value(row, ["indice", "indice_reclamacoes", "indice_de_reclamacoes"]))
        regulated_procedent = try_int(
            pick_value(row, ["quantidade_de_reclamacoes_reguladas_procedentes", "reclamacoes_reguladas_procedentes"])
        )
        regulated_other = try_int(
            pick_value(row, ["quantidade_de_reclamacoes_reguladas_outras", "reclamacoes_reguladas_outras"])
        )
        unregulated = try_int(
            pick_value(row, ["quantidade_de_reclamacoes_nao_reguladas", "reclamacoes_nao_reguladas"])
        )
        total_complaints = try_int(
            pick_value(row, ["quantidade_total_de_reclamacoes", "total_de_reclamacoes", "reclamacoes", "qtde_reclamacoes"])
        )
        clients = try_int(
            pick_value(
                row,
                [
                    "quantidade_de_clientes_consorciados",
                    "clientes_consorciados",
                    "clientes",
                    "quantidade_clientes",
                    "base_clientes",
                ],
            )
        )

        position = try_int(pick_value(row, ["posicao", "ranking", "classificacao"])) or idx

        parsed.append(
            {
                "institution_name": str(name),
                "normalized_name": normalize_text(str(name)),
                "cnpj": cnpj_root,
                "cnpj_root": cnpj_root,
                "position": position,
                "index_value": index_value,
                "complaints_regulated_procedent": regulated_procedent,
                "complaints_regulated_other": regulated_other,
                "complaints_unregulated": unregulated,
                "complaints": total_complaints,
                "clients": clients,
                "ano": try_int(pick_value(row, ["ano"])),
                "semestre": str(pick_value(row, ["semestre"]) or "").strip() or None,
                "raw": row,
            }
        )

    parsed.sort(key=lambda x: (x["position"] is None, x["position"] or 999999, x.get("institution_name") or ""))

    meta["exists"] = True
    meta["parsed_count"] = len(parsed)

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
        cnpj = row.get("cnpj_root") or row.get("cnpj")
        if cnpj:
            lookup[f"cnpj:{cnpj}"] = row

        key = row.get("normalized_name")
        if key and f"name:{key}" not in lookup:
            lookup[f"name:{key}"] = row

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
        cnpj_root = re.sub(r"\D+", "", str(inst.get("cnpj") or "")) or None
        branch_info = branch_counts.get(key, {})

        ranking_info = {}
        if cnpj_root and f"cnpj:{cnpj_root}" in ranking_lookup:
            ranking_info = ranking_lookup[f"cnpj:{cnpj_root}"]
        elif f"name:{key}" in ranking_lookup:
            ranking_info = ranking_lookup[f"name:{key}"]

        score = compute_institution_score(
            status=inst.get("status"),
            branch_count=branch_info.get("branch_count", 0),
            ranking_position=ranking_info.get("position"),
            scoring_rules=scoring_rules,
        )

        items.append(
            {
                "institution_id": inst.get("institution_id"),
                "cnpj": cnpj_root,
                "cnpj_root": cnpj_root,
                "institution_name": inst["institution_name"],
                "normalized_name": key,
                "status": inst.get("status"),
                "branch_count": branch_info.get("branch_count", 0),
                "ufs": branch_info.get("ufs", []),
                "cities_sample": branch_info.get("cities_sample", []),
                "ranking_position": ranking_info.get("position"),
                "ranking_index": ranking_info.get("index_value"),
                "ranking_complaints": ranking_info.get("complaints"),
                "ranking_clients": ranking_info.get("clients"),
                "ranking_complaints_regulated_procedent": ranking_info.get("complaints_regulated_procedent"),
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
    """
    Gera produtos reais a partir do ConsorcioBD mensal do Banco Central.

    product_rules é mantido apenas como apoio semântico/SEO em semantic_products.
    Os itens principais de produtos.json vêm dos CSVs oficiais do ConsorcioBD.
    """
    raw_zip_path = Path("data/raw/bc/consorciobd/latest_source.bin")
    generated_at = utc_now_iso()

    segment_labels = {
        "1": "Consórcio de veículos",
        "2": "Consórcio de motocicletas",
        "3": "Consórcio de imóveis",
        "4": "Consórcio de serviços",
        "5": "Consórcio de eletroeletrônicos",
        "6": "Consórcio de outros bens móveis",
    }

    segment_keys = {
        "1": "veiculos",
        "2": "motos",
        "3": "imobiliario",
        "4": "servicos",
        "5": "eletroeletronicos",
        "6": "outros_bens_moveis",
    }

    def infer_segment_from_member(member_name: str, row: Dict[str, Any]) -> Tuple[str, str]:
        member_norm = normalize_text(member_name)
        codigo = str(pick_value(row, ["codigo_do_segmento", "segmento", "codigo_segmento"]) or "").strip()
        codigo = re.sub(r"\D+", "", codigo)

        if "imoveis" in member_norm or "imovel" in member_norm:
            return "imobiliario", "Consórcio de imóveis"
        if "moto" in member_norm:
            return "motos", "Consórcio de motocicletas"
        if "veiculo" in member_norm or "automovel" in member_norm or "auto" in member_norm:
            return "veiculos", "Consórcio de veículos"
        if "servico" in member_norm:
            return "servicos", "Consórcio de serviços"

        if codigo:
            key = segment_keys.get(codigo, f"segmento_{codigo}")
            label = segment_labels.get(codigo, f"Consórcio — segmento {codigo}")
            return key, label

        return "consorcio", "Consórcio"

    def weighted_average(total_value: float, total_weight: float) -> Optional[float]:
        if total_weight <= 0:
            return None
        return round(total_value / total_weight, 4)

    groups: Dict[str, Dict[str, Any]] = {}

    if raw_zip_path.exists():
        with zipfile.ZipFile(raw_zip_path, "r") as zf:
            for member in zf.namelist():
                if not member.lower().endswith(".csv"):
                    continue

                member_norm = normalize_text(member)
                if "significado" in member_norm or "layout" in member_norm:
                    continue

                with zf.open(member, "r") as fh:
                    rows, _meta = parse_csv_bytes(fh.read())

                for row in rows:
                    cnpj_root = re.sub(
                        r"\D+",
                        "",
                        str(pick_value(row, ["cnpj_da_administradora", "cnpj", "cnpj_ac"]) or ""),
                    ) or None

                    nome_admin = normalize_spaces(
                        pick_value(row, ["nome_da_administradora", "administradora_de_consorcio", "administradora"])
                    )

                    if not cnpj_root and not nome_admin:
                        continue

                    categoria_key, categoria_label = infer_segment_from_member(member, row)

                    taxa = try_float(pick_value(row, ["taxa_de_administracao", "taxa_administracao"]))
                    prazo = try_float(pick_value(row, ["prazo_do_grupo_em_meses", "prazo_meses", "prazo"]))
                    valor_bem = try_float(pick_value(row, ["valor_medio_do_bem", "valor_credito", "valor_medio"]))

                    # Não publicar zeros/fallbacks como se fossem informação comercial real.
                    # No ConsorcioBD, ausência de campo ou zero em linha consolidada pode significar
                    # inexistência de dado útil para comparação, não uma taxa/prazo/valor real.
                    if taxa is not None and taxa <= 0:
                        taxa = None
                    if prazo is not None and prazo <= 0:
                        prazo = None
                    if valor_bem is not None and valor_bem <= 0:
                        valor_bem = None

                    cotas_ativas = try_float(
                        pick_value(
                            row,
                            [
                                "quantidade_de_cotas_ativas_em_dia",
                                "quantidade_de_cotas_ativas_nao_contempladas",
                                "quantidade_acumulada_de_cotas_ativas_contempladas",
                                "quantidade_de_cotas_comercializadas_no_mes",
                            ],
                        )
                    )

                    grupos_ativos = try_float(pick_value(row, ["quantidade_de_grupos_ativos"]))
                    contempladas_mes = try_float(pick_value(row, ["quantidade_de_cotas_ativas_contempladas_no_mes"]))
                    inadimplentes = (
                        try_float(pick_value(row, ["quantidade_de_cotas_ativas_contempladas_inadimplentes"])) or 0.0
                    ) + (
                        try_float(pick_value(row, ["quantidade_de_cotas_ativas_nao_contempladas_inadimplentes"])) or 0.0
                    )

                    has_operational_signal = any(
                        value is not None and value > 0
                        for value in (taxa, prazo, valor_bem, cotas_ativas, grupos_ativos, contempladas_mes, inadimplentes)
                    )
                    if not has_operational_signal:
                        continue

                    weight = cotas_ativas if cotas_ativas is not None and cotas_ativas > 0 else 1.0

                    group_key = f"{cnpj_root or normalize_text(nome_admin)}::{categoria_key}"

                    if group_key not in groups:
                        groups[group_key] = {
                            "key": categoria_key,
                            "categoria_key": categoria_key,
                            "categoria_label": categoria_label,
                            "nome": categoria_label,
                            "label": categoria_label,
                            "cnpj_da_administradora": cnpj_root,
                            "cnpj_root": cnpj_root,
                            "nome_da_administradora": nome_admin,
                            "data_base": pick_value(row, ["data_base"]),
                            "codigo_do_segmento": pick_value(row, ["codigo_do_segmento"]),
                            "source_members": set(),
                            "row_count": 0,
                            "quantidade_de_grupos_ativos": 0.0,
                            "quantidade_de_cotas_ativas_em_dia": 0.0,
                            "quantidade_de_cotas_ativas_contempladas_no_mes": 0.0,
                            "quantidade_de_cotas_inadimplentes": 0.0,
                            "_taxa_total": 0.0,
                            "_taxa_weight": 0.0,
                            "_prazo_total": 0.0,
                            "_prazo_weight": 0.0,
                            "_valor_total": 0.0,
                            "_valor_weight": 0.0,
                        }

                    item = groups[group_key]
                    item["source_members"].add(member)
                    item["row_count"] += 1
                    item["quantidade_de_grupos_ativos"] += grupos_ativos or 0.0
                    item["quantidade_de_cotas_ativas_em_dia"] += cotas_ativas or 0.0
                    item["quantidade_de_cotas_ativas_contempladas_no_mes"] += contempladas_mes or 0.0
                    item["quantidade_de_cotas_inadimplentes"] += inadimplentes or 0.0

                    if taxa is not None:
                        item["_taxa_total"] += taxa * weight
                        item["_taxa_weight"] += weight
                    if prazo is not None:
                        item["_prazo_total"] += prazo * weight
                        item["_prazo_weight"] += weight
                    if valor_bem is not None:
                        item["_valor_total"] += valor_bem * weight
                        item["_valor_weight"] += weight

    items = []

    for item in groups.values():
        taxa_media = weighted_average(item["_taxa_total"], item["_taxa_weight"])
        prazo_medio = weighted_average(item["_prazo_total"], item["_prazo_weight"])
        valor_medio = weighted_average(item["_valor_total"], item["_valor_weight"])

        clean = {
            "id": slugify(f"{item.get('cnpj_root') or item.get('nome_da_administradora')}-{item['categoria_key']}"),
            "key": item["categoria_key"],
            "nome": item["nome"],
            "label": item["label"],
            "categoria": item["categoria_label"],
            "categoria_key": item["categoria_key"],
            "categoria_label": item["categoria_label"],
            "cnpj_da_administradora": item.get("cnpj_da_administradora"),
            "cnpj_root": item.get("cnpj_root"),
            "nome_da_administradora": item.get("nome_da_administradora"),
            "data_base": item.get("data_base"),
            "codigo_do_segmento": item.get("codigo_do_segmento"),
            "taxa_de_administracao": taxa_media,
            "taxa_administracao": taxa_media,
            "prazo_do_grupo_em_meses": prazo_medio,
            "prazo_meses": prazo_medio,
            "valor_medio_do_bem": valor_medio,
            "valor_credito": valor_medio,
            "quantidade_de_grupos_ativos": int(item["quantidade_de_grupos_ativos"]) if item["quantidade_de_grupos_ativos"] else None,
            "quantidade_de_cotas_ativas_em_dia": int(item["quantidade_de_cotas_ativas_em_dia"]) if item["quantidade_de_cotas_ativas_em_dia"] else None,
            "quantidade_de_cotas_ativas_contempladas_no_mes": int(item["quantidade_de_cotas_ativas_contempladas_no_mes"]) if item["quantidade_de_cotas_ativas_contempladas_no_mes"] else None,
            "quantidade_de_cotas_inadimplentes": int(item["quantidade_de_cotas_inadimplentes"]) if item["quantidade_de_cotas_inadimplentes"] else None,
            "row_count": item["row_count"],
            "source_members": sorted(item["source_members"]),
            "source_coverage": {
                "monthly_consolidated": monthly_summary.get("exists", False),
                "quarterly_uf": uf_summary.get("exists", False),
                "quarterly_adm": adm_summary.get("exists", False),
            },
        }

        items.append(clean)

    items.sort(key=lambda x: (x.get("categoria_label") or "", x.get("nome_da_administradora") or ""))

    semantic_products = []
    route_slugs = [route["slug"] for route in seo_routes]

    for product in product_rules:
        aliases = [normalize_text(product["label"])] + [normalize_text(x) for x in product.get("aliases", [])]
        related_routes = []

        for slug in route_slugs:
            slug_norm = normalize_text(slug)
            if product["key"] in slug_norm or any(alias and alias in slug_norm for alias in aliases):
                related_routes.append(slug)

        semantic_products.append(
            {
                "key": product["key"],
                "label": product["label"],
                "aliases": product.get("aliases", []),
                "intents": product.get("intents", []),
                "related_routes": related_routes,
            }
        )

    return {
        "metadata": {
            "generated_at": generated_at,
            "count": len(items),
            "semantic_products_count": len(semantic_products),
            "source": "Banco Central do Brasil — ConsorcioBD",
            "monthly_summary": monthly_summary,
            "uf_summary": uf_summary,
            "adm_summary": adm_summary,
        },
        "items": items,
        "semantic_products": semantic_products,
    }


def safe_div(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def percentile_score(value: Optional[float], values: List[float], *, inverse: bool = False, log_scale: bool = False) -> Optional[float]:
    if value is None or not values:
        return None

    valid = [v for v in values if v is not None and v >= 0]
    if not valid:
        return None

    if log_scale:
        import math
        value = math.log1p(max(value, 0))
        valid = [math.log1p(max(v, 0)) for v in valid]

    low = min(valid)
    high = max(valid)
    if high == low:
        return 50.0

    raw = (value - low) / (high - low) * 100
    raw = max(0.0, min(100.0, raw))
    if inverse:
        raw = 100.0 - raw
    return round(raw, 2)


def build_administradoras(
    instituicoes_payload: Dict[str, Any],
    produtos_payload: Dict[str, Any],
    rankings_payload: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Novo contrato principal do modelo híbrido.

    administradoras.json é o read model público de ranking/diagnóstico.
    Ele usa cnpj_root como chave técnica, mas não pressupõe CNPJ completo.
    """
    products_by_cnpj: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for product in produtos_payload.get("items", []):
        cnpj = product.get("cnpj_root") or product.get("cnpj_da_administradora")
        if cnpj:
            products_by_cnpj[str(cnpj)].append(product)

    rankings_by_cnpj: Dict[str, Dict[str, Any]] = {}
    rankings_by_name: Dict[str, Dict[str, Any]] = {}
    for ranking in rankings_payload.get("items", []):
        cnpj = ranking.get("cnpj_root") or ranking.get("cnpj")
        if cnpj:
            rankings_by_cnpj[str(cnpj)] = ranking
        name_key = ranking.get("normalized_name")
        if name_key and name_key not in rankings_by_name:
            rankings_by_name[str(name_key)] = ranking

    operational_rows = []
    for products in products_by_cnpj.values():
        cotas = sum((p.get("quantidade_de_cotas_ativas_em_dia") or 0) for p in products)
        grupos = sum((p.get("quantidade_de_grupos_ativos") or 0) for p in products)
        contempl = sum((p.get("quantidade_de_cotas_ativas_contempladas_no_mes") or 0) for p in products)
        inad = sum((p.get("quantidade_de_cotas_inadimplentes") or 0) for p in products)
        operational_rows.append({"cotas": cotas, "grupos": grupos, "contemplacoes": contempl, "inadimplentes": inad})

    cotas_values = [x["cotas"] for x in operational_rows if x["cotas"] > 0]
    grupos_values = [x["grupos"] for x in operational_rows if x["grupos"] > 0]
    contemplacoes_values = [x["contemplacoes"] for x in operational_rows if x["contemplacoes"] > 0]
    inad_ratios = [safe_div(x["inadimplentes"], x["cotas"]) for x in operational_rows]
    inad_ratio_values = [x for x in inad_ratios if x is not None]
    ranking_index_values = [r.get("index_value") for r in rankings_payload.get("items", []) if r.get("index_value") is not None]
    branch_values = [i.get("branch_count") for i in instituicoes_payload.get("items", []) if (i.get("branch_count") or 0) > 0]

    items = []

    for inst in instituicoes_payload.get("items", []):
        cnpj_root = inst.get("cnpj_root") or inst.get("cnpj")
        normalized_name = inst.get("normalized_name") or normalize_text(inst.get("institution_name"))
        products = products_by_cnpj.get(str(cnpj_root), []) if cnpj_root else []
        ranking = rankings_by_cnpj.get(str(cnpj_root)) if cnpj_root else None
        if ranking is None and normalized_name:
            ranking = rankings_by_name.get(str(normalized_name))

        segmentos = sorted({p.get("categoria_key") for p in products if p.get("categoria_key")})
        grupos_ativos = sum((p.get("quantidade_de_grupos_ativos") or 0) for p in products)
        cotas_ativas = sum((p.get("quantidade_de_cotas_ativas_em_dia") or 0) for p in products)
        contemplacoes_mes = sum((p.get("quantidade_de_cotas_ativas_contempladas_no_mes") or 0) for p in products)
        inadimplentes = sum((p.get("quantidade_de_cotas_inadimplentes") or 0) for p in products)
        inad_ratio = safe_div(inadimplentes, cotas_ativas)

        taxa_items = [p.get("taxa_administracao") for p in products if p.get("taxa_administracao") is not None]
        prazo_items = [p.get("prazo_meses") for p in products if p.get("prazo_meses") is not None]
        credito_items = [p.get("valor_credito") for p in products if p.get("valor_credito") is not None]

        branch_count = inst.get("branch_count") or 0
        ranking_index = ranking.get("index_value") if ranking else inst.get("ranking_index")
        ranking_complaints = ranking.get("complaints") if ranking else inst.get("ranking_complaints")
        ranking_clients = ranking.get("clients") if ranking else inst.get("ranking_clients")

        escala_cotas = percentile_score(cotas_ativas if cotas_ativas > 0 else None, cotas_values, log_scale=True)
        escala_grupos = percentile_score(grupos_ativos if grupos_ativos > 0 else None, grupos_values, log_scale=True)
        escala_score_parts = [x for x in (escala_cotas, escala_grupos) if x is not None]
        escala_score = round(sum(escala_score_parts) / len(escala_score_parts), 2) if escala_score_parts else None

        atividade_score = percentile_score(
            contemplacoes_mes if contemplacoes_mes > 0 else None,
            contemplacoes_values,
            log_scale=True,
        )
        inad_score = percentile_score(inad_ratio, inad_ratio_values, inverse=True) if inad_ratio is not None else None
        operacao_parts = [x for x in (atividade_score, inad_score) if x is not None]
        operacao_score = round(sum(operacao_parts) / len(operacao_parts), 2) if operacao_parts else None

        reputacao_score = percentile_score(ranking_index, ranking_index_values, inverse=True) if ranking_index is not None else None
        presenca_score = percentile_score(branch_count if branch_count > 0 else None, branch_values, log_scale=True)

        weighted_parts = []
        for score, weight in ((escala_score, 0.30), (operacao_score, 0.25), (reputacao_score, 0.30), (presenca_score, 0.15)):
            if score is not None:
                weighted_parts.append((score, weight))
        if weighted_parts:
            total_weight = sum(w for _, w in weighted_parts)
            geral_score = round(sum(score * weight for score, weight in weighted_parts) / total_weight, 2)
        else:
            geral_score = inst.get("score")

        data_quality_flags = []
        if not products:
            data_quality_flags.append("sem_operacao_consorciobd_vinculada")
        if ranking is None:
            data_quality_flags.append("sem_ranking_reclamacoes_vinculado")
        if ranking_index is None:
            data_quality_flags.append("sem_indice_bc_calculado")

        items.append(
            {
                "id": cnpj_root or slugify(inst.get("institution_name") or "administradora"),
                "cnpj_root": cnpj_root,
                "nome": inst.get("institution_name"),
                "nome_normalizado": normalized_name,
                "status": inst.get("status"),
                "ufs": inst.get("ufs", []),
                "segmentos_ativos": segmentos,
                "presenca_score": presenca_score,
                "escala": {
                    "grupos_ativos": int(grupos_ativos) if grupos_ativos else None,
                    "cotas_ativas": int(cotas_ativas) if cotas_ativas else None,
                },
                "operacao": {
                    "contemplacoes_mes": int(contemplacoes_mes) if contemplacoes_mes else None,
                    "inadimplentes": int(inadimplentes) if inadimplentes else None,
                    "inadimplencia_ratio": round(inad_ratio, 6) if inad_ratio is not None else None,
                },
                "reputacao": {
                    "bc_index": ranking_index,
                    "reclamacoes": ranking_complaints,
                    "clientes": ranking_clients,
                    "ranking_position": ranking.get("position") if ranking else inst.get("ranking_position"),
                    "periodo": {
                        "ano": ranking.get("ano") if ranking else None,
                        "semestre": ranking.get("semestre") if ranking else None,
                    },
                },
                "benchmarks": {
                    "taxa_adm_aprox_pct": round(sum(taxa_items) / len(taxa_items), 4) if taxa_items else None,
                    "prazo_medio_meses": round(sum(prazo_items) / len(prazo_items), 2) if prazo_items else None,
                    "credito_medio": round(sum(credito_items) / len(credito_items), 2) if credito_items else None,
                    "observacao": "Benchmarks derivados de dados agregados; não equivalem a oferta comercial individual.",
                },
                "scores": {
                    "geral": geral_score,
                    "escala": escala_score,
                    "operacao": operacao_score,
                    "reputacao": reputacao_score,
                    "presenca": presenca_score,
                },
                "data_quality_flags": data_quality_flags,
                "legacy": inst,
            }
        )

    items.sort(key=lambda x: (-(x.get("scores", {}).get("geral") or 0), x.get("nome") or ""))

    return {
        "metadata": {
            "generated_at": utc_now_iso(),
            "count": len(items),
            "contract": "administradoras.v1",
            "source": "BCB Cadastro + ConsorcioBD + Ranking de Reclamações",
        },
        "items": items,
    }


def build_segmentos(produtos_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Gera visão agregada por segmento/modalidade para o modelo híbrido."""
    grouped: Dict[str, Dict[str, Any]] = {}

    priority = {
        "veiculos": 1,
        "imobiliario": 2,
        "motos": 3,
        "servicos": 4,
        "outros_bens_moveis": 8,
        "eletroeletronicos": 9,
    }

    for product in produtos_payload.get("items", []):
        key = product.get("categoria_key") or product.get("key") or "consorcio"
        label = product.get("categoria_label") or product.get("categoria") or product.get("label") or key
        row = grouped.setdefault(
            key,
            {
                "segmento": key,
                "label": label,
                "priority": priority.get(key, 99),
                "administradoras": set(),
                "source_members": set(),
                "grupos_ativos": 0,
                "cotas_ativas": 0,
                "contemplacoes_mes": 0,
                "inadimplentes": 0,
                "taxas": [],
                "prazos": [],
                "creditos": [],
                "row_count": 0,
            },
        )
        row["row_count"] += 1
        if product.get("cnpj_root"):
            row["administradoras"].add(product.get("cnpj_root"))
        for member in product.get("source_members") or []:
            row["source_members"].add(member)
        row["grupos_ativos"] += product.get("quantidade_de_grupos_ativos") or 0
        row["cotas_ativas"] += product.get("quantidade_de_cotas_ativas_em_dia") or 0
        row["contemplacoes_mes"] += product.get("quantidade_de_cotas_ativas_contempladas_no_mes") or 0
        row["inadimplentes"] += product.get("quantidade_de_cotas_inadimplentes") or 0
        if product.get("taxa_administracao") is not None:
            row["taxas"].append(product.get("taxa_administracao"))
        if product.get("prazo_meses") is not None:
            row["prazos"].append(product.get("prazo_meses"))
        if product.get("valor_credito") is not None:
            row["creditos"].append(product.get("valor_credito"))

    items = []
    for row in grouped.values():
        cotas = row["cotas_ativas"]
        inad_ratio = safe_div(row["inadimplentes"], cotas)
        items.append(
            {
                "segmento": row["segmento"],
                "label": row["label"],
                "priority": row["priority"],
                "administradoras_count": len(row["administradoras"]),
                "operacao": {
                    "grupos_ativos": int(row["grupos_ativos"]) if row["grupos_ativos"] else None,
                    "cotas_ativas": int(row["cotas_ativas"]) if row["cotas_ativas"] else None,
                    "contemplacoes_mes": int(row["contemplacoes_mes"]) if row["contemplacoes_mes"] else None,
                    "inadimplentes": int(row["inadimplentes"]) if row["inadimplentes"] else None,
                    "inadimplencia_ratio": round(inad_ratio, 6) if inad_ratio is not None else None,
                },
                "benchmarks_observados": {
                    "taxa_adm_aprox_pct": round(sum(row["taxas"]) / len(row["taxas"]), 4) if row["taxas"] else None,
                    "prazo_medio_meses": round(sum(row["prazos"]) / len(row["prazos"]), 2) if row["prazos"] else None,
                    "credito_medio": round(sum(row["creditos"]) / len(row["creditos"]), 2) if row["creditos"] else None,
                    "observacao": "Dados agregados; úteis para contexto, não para promessa de oferta individual.",
                },
                "source_members": sorted(row["source_members"]),
                "row_count": row["row_count"],
            }
        )

    items.sort(key=lambda x: (x["priority"], x["label"]))

    return {
        "metadata": {
            "generated_at": utc_now_iso(),
            "count": len(items),
            "contract": "segmentos.v1",
            "source": "BCB ConsorcioBD",
        },
        "items": items,
    }


def build_ofertas(raw_base: Path) -> Dict[str, Any]:
    """
    Contrato reservado para ofertas comerciais reais.

    Se futuramente houver collectors/simuladores.py, ele pode gravar JSONs em
    data/raw/market/offers/*.json. Sem isso, o arquivo sai vazio e honesto.
    """
    offers_dir = raw_base / "market" / "offers"
    items: List[Dict[str, Any]] = []

    if offers_dir.exists():
        for path in sorted(offers_dir.glob("*.json")):
            payload = try_load_json(path)
            records = extract_first_list_of_dicts(payload)
            if isinstance(payload, dict) and not records and any(k in payload for k in ("administradora", "segmento", "plano")):
                records = [payload]
            for record in records:
                if not isinstance(record, dict):
                    continue
                row = dict(record)
                row.setdefault("fonte_tipo", "oferta_comercial")
                row.setdefault("source_file", str(path))
                items.append(row)

    return {
        "metadata": {
            "generated_at": utc_now_iso(),
            "count": len(items),
            "contract": "ofertas.v1",
            "source": "Simuladores e páginas comerciais, quando coletados",
            "status": "empty_until_market_collectors_are_enabled" if not items else "available",
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

    def add(kind: str, label: Any, target: str, extra: Optional[Dict[str, Any]] = None) -> None:
        label = normalize_spaces(str(label)) if label is not None else None
        if not label:
            return

        key = (kind, normalize_text(label), target)
        if key in seen:
            return

        seen.add(key)
        row = {"type": kind, "label": label, "target": target}
        if extra:
            row.update(extra)
        suggestions.append(row)

    for item in instituicoes_payload.get("items", [])[:150]:
        add(
            "instituicao",
            item.get("institution_name"),
            "/financas/consorcio/ranking-administradoras.php",
            {"cnpj_root": item.get("cnpj_root") or item.get("cnpj")},
        )

    for item in produtos_payload.get("items", []):
        add(
            "produto",
            item.get("categoria_label") or item.get("label") or item.get("nome"),
            "/financas/consorcio/",
            {"categoria_key": item.get("categoria_key") or item.get("key")},
        )

    for item in produtos_payload.get("semantic_products", []):
        related_routes = item.get("related_routes") or []
        target = "/financas/consorcio/"
        if related_routes:
            slug = related_routes[0]
            target = f"/financas/consorcio/{slug}/" if slug not in ("melhor-consorcio", "consorcio-ou-financiamento", "ranking-administradoras", "menor-taxa") else f"/financas/consorcio/{slug}.php"

        add("produto", item.get("label"), target, {"semantic": True})

    for route in seo_routes:
        add("rota", route["title"], route["path"], {"primary_keyword": route.get("primary_keyword")})

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
    administradoras_payload: Dict[str, Any],
    segmentos_payload: Dict[str, Any],
    ofertas_payload: Dict[str, Any],
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
        "contracts": {
            "administradoras": "administradoras.v1",
            "segmentos": "segmentos.v1",
            "ofertas": "ofertas.v1",
            "produtos": "produtos.legacy_compatible",
        },
        "counts": {
            "instituicoes": len(instituicoes_payload.get("items", [])),
            "administradoras": len(administradoras_payload.get("items", [])),
            "segmentos": len(segmentos_payload.get("items", [])),
            "ofertas": len(ofertas_payload.get("items", [])),
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
            "administradoras": sha256_text(dump_json_text(administradoras_payload)),
            "segmentos": sha256_text(dump_json_text(segmentos_payload)),
            "ofertas": sha256_text(dump_json_text(ofertas_payload)),
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
    semantic_products = produtos_payload.get("semantic_products", [])
    rankings = rankings_payload.get("items", [])
    cenarios = cenarios_payload.get("items", [])

    payloads = {}

    for route in seo_routes:
        slug = route["slug"]
        slug_norm = normalize_text(slug)

        related_products = []

        for product in products:
            product_key = product.get("categoria_key") or product.get("key") or ""
            product_label = normalize_text(product.get("categoria_label") or product.get("label") or product.get("nome") or "")

            if product_key and product_key in slug_norm:
                related_products.append(product)
            elif product_label and product_label in slug_norm:
                related_products.append(product)

        for product in semantic_products:
            aliases = [normalize_text(product.get("label") or "")] + [normalize_text(alias) for alias in product.get("aliases", [])]
            if product.get("key") in slug_norm or any(alias and alias in slug_norm for alias in aliases):
                if product not in related_products:
                    related_products.append(product)

        if not related_products and slug in ("carro", "moto", "imobiliario"):
            key_map = {"carro": "veiculos", "moto": "motos", "imobiliario": "imobiliario"}
            wanted = key_map.get(slug)
            related_products = [p for p in products if p.get("categoria_key") == wanted][:50]

        route_payload = {
            "route": route,
            "snapshot": {
                "instituicoes_count": len(institutions),
                "ranking_count": len(rankings),
                "products_count": len(products),
            },
            "top_instituicoes": institutions[:10],
            "top_rankings": rankings[:10],
            "related_products": related_products[:80],
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
    administradoras_payload = build_administradoras(instituicoes_payload, produtos_payload, rankings_payload)
    segmentos_payload = build_segmentos(produtos_payload)
    ofertas_payload = build_ofertas(raw_base)
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
            "administradoras.json": administradoras_payload,
            "segmentos.json": segmentos_payload,
            "ofertas.json": ofertas_payload,
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
        administradoras_payload=administradoras_payload,
        segmentos_payload=segmentos_payload,
        ofertas_payload=ofertas_payload,
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
    changed |= write_json_if_changed(dist_global_dir / "administradoras.json", administradoras_payload)
    changed |= write_json_if_changed(dist_global_dir / "segmentos.json", segmentos_payload)
    changed |= write_json_if_changed(dist_global_dir / "ofertas.json", ofertas_payload)
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
            "administradoras": len(administradoras_payload.get("items", [])),
            "segmentos": len(segmentos_payload.get("items", [])),
            "ofertas": len(ofertas_payload.get("items", [])),
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
