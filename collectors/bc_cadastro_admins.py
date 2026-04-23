#!/usr/bin/env python3
"""
Coletor do cadastro de administradoras / sedes de consórcio via BCB Olinda OData.

Responsabilidades desta etapa:
- ler config/sources.json
- buscar a fonte bc_cadastro_admins com paginação OData
- tentar fallback oficial em CSV quando o JSON falhar
- gerar um raw canônico em data/raw/bc/cadastro/latest.json
- gerar uma versão normalizada em data/stage/cadastro/instituicoes_cadastro.json
- evitar reescrita desnecessária quando o conteúdo não mudou
- expor changed=true/false para o GitHub Actions via GITHUB_OUTPUT

Este coletor não gera data/dist. Isso fica para o build-read-models.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import sys
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from unidecode import unidecode


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def dump_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2, sort_keys=False)
        fh.write("\n")


def stable_json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_of(data: Any) -> str:
    return hashlib.sha256(stable_json_dumps(data).encode("utf-8")).hexdigest()


def normalize_key(value: str) -> str:
    value = unidecode(str(value or ""))
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "", value)
    return value


def normalize_spaces(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    return text or None


def digits_only(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    digits = re.sub(r"\D+", "", str(value))
    return digits or None


def normalize_name_key(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = normalize_spaces(value)
    if not text:
        return None
    text = unidecode(text).upper()
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def normalize_uf(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = normalize_spaces(value)
    if not text:
        return None
    text = unidecode(text).upper()
    return text[:2] if len(text) >= 2 else text


def build_session(timeout_seconds: int) -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.request_timeout = timeout_seconds  # type: ignore[attr-defined]
    return session


def append_github_output(name: str, value: str) -> None:
    github_output = os.environ.get("GITHUB_OUTPUT")
    if not github_output:
        return
    with open(github_output, "a", encoding="utf-8") as fh:
        fh.write(f"{name}={value}\n")


def get_source_config(config: Dict[str, Any], source_name: str) -> Dict[str, Any]:
    try:
        return config["sources"][source_name]
    except KeyError as exc:
        raise KeyError(f"Fonte '{source_name}' não encontrada em config/sources.json.") from exc


def extract_odata_records(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(payload.get("value"), list):
        return payload["value"]

    d_obj = payload.get("d")
    if isinstance(d_obj, dict):
        results = d_obj.get("results")
        if isinstance(results, list):
            return results

    raise ValueError("Resposta OData sem lista de registros em 'value' ou 'd.results'.")


def fetch_paginated_odata_resilient(
    session: requests.Session,
    endpoint: str,
    default_params: Dict[str, Any],
    pagination_param: str,
    headers: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], str]:
    """
    Tenta OData JSON com lotes decrescentes.
    Retorna (records, page_meta, mode_used).
    """
    base_top = int(default_params.get("$top", 1000) or 1000)
    candidate_tops = [base_top, 200, 100, 50]

    seen: set[int] = set()
    candidate_tops = [value for value in candidate_tops if not (value in seen or seen.add(value))]

    last_error: Optional[Exception] = None

    for top in candidate_tops:
        try:
            all_records: List[Dict[str, Any]] = []
            page_meta: List[Dict[str, Any]] = []
            skip = int(default_params.get("$skip", 0) or 0)

            while True:
                params = deepcopy(default_params)
                params["$format"] = "json"
                params["$top"] = top
                params[pagination_param] = skip

                response = session.get(
                    endpoint,
                    params=params,
                    headers=headers,
                    timeout=getattr(session, "request_timeout", 60),
                )
                response.raise_for_status()

                payload = response.json()
                records = extract_odata_records(payload)

                page_meta.append(
                    {
                        "mode": "odata-json",
                        "skip": skip,
                        "top": top,
                        "returned_records": len(records),
                        "url": response.url,
                        "status_code": response.status_code,
                    }
                )

                if not records:
                    break

                all_records.extend(records)

                if len(records) < top:
                    break

                skip += len(records)

            return all_records, page_meta, "odata-json"

        except Exception as exc:
            last_error = exc
            time.sleep(2)

    if last_error:
        raise last_error

    raise RuntimeError("Falha inesperada ao coletar OData JSON.")


def fetch_csv_fallback(
    session: requests.Session,
    endpoint: str,
    headers: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], str]:
    params = {"$format": "text/csv"}
    headers_csv = dict(headers)
    headers_csv["Accept"] = "text/csv"

    response = session.get(
        endpoint,
        params=params,
        headers=headers_csv,
        timeout=getattr(session, "request_timeout", 60),
    )
    response.raise_for_status()

    text = response.text.lstrip("\ufeff")
    rows = list(csv.DictReader(io.StringIO(text)))

    page_meta = [
        {
            "mode": "odata-csv-fallback",
            "returned_records": len(rows),
            "url": response.url,
            "status_code": response.status_code,
        }
    ]
    return rows, page_meta, "odata-csv-fallback"


def build_lookup(record: Dict[str, Any]) -> Dict[str, Any]:
    return {normalize_key(key): value for key, value in record.items()}


def pick_first(record: Dict[str, Any], candidates: Iterable[str]) -> Any:
    lookup = build_lookup(record)
    for candidate in candidates:
        key = normalize_key(candidate)
        if key in lookup:
            return lookup[key]
    return None


def infer_active(status_text: Optional[str]) -> Optional[bool]:
    if not status_text:
        return None

    text = unidecode(status_text).upper()

    negative_markers = [
        "INATIV",
        "CANCEL",
        "SUSPENS",
        "ENCERR",
        "BAIXA",
        "EXTINT",
    ]
    if any(marker in text for marker in negative_markers):
        return False

    positive_markers = [
        "ATIV",
        "AUTORIZ",
        "FUNCION",
        "REGULAR",
    ]
    if any(marker in text for marker in positive_markers):
        return True

    return None


def normalize_record(record: Dict[str, Any], index: int) -> Dict[str, Any]:
    institution_id = pick_first(
        record,
        [
            "Codigo",
            "Código",
            "CodigoInstituicao",
            "CódigoInstituicao",
            "Codigo_IF",
            "CodInst",
            "CodIf",
            "ISPB",
        ],
    )
    institution_name = pick_first(
        record,
        [
            "NomeInstituicao",
            "Nome Instituicao",
            "Nome",
            "Instituicao",
            "Instituição",
            "NomeIF",
            "NomeFantasia",
            "NomeReduzido",
        ],
    )
    trade_name = pick_first(
        record,
        [
            "NomeFantasia",
            "NomeReduzido",
            "NomeAbreviado",
        ],
    )
    cnpj = pick_first(
        record,
        [
            "CNPJ",
            "Cnpj",
            "NumeroCNPJ",
            "NúmeroCNPJ",
            "CNPJCompleto",
        ],
    )
    segment = pick_first(
        record,
        [
            "Segmento",
            "TipoInstituicao",
            "TipoInstituição",
            "Classe",
            "Categoria",
        ],
    )
    conglomerate_name = pick_first(
        record,
        [
            "NomeConglomerado",
            "Conglomerado",
            "NomeGrupo",
            "Grupo",
        ],
    )
    status_text = pick_first(
        record,
        [
            "Situacao",
            "Situação",
            "Status",
            "SituacaoRegistro",
            "SituaçãoRegistro",
            "SituacaoInstituicao",
            "SituaçãoInstituição",
        ],
    )
    city = pick_first(
        record,
        [
            "Municipio",
            "Município",
            "Cidade",
            "NomeMunicipio",
            "NomeMunicípio",
        ],
    )
    uf = pick_first(record, ["UF", "Uf", "SiglaUF", "SiglaUf"])
    start_date = pick_first(
        record,
        [
            "DataInicio",
            "DataInício",
            "DataAutorizacao",
            "DataAutorização",
            "InicioOperacao",
            "InícioOperação",
        ],
    )

    institution_id_text = normalize_spaces(institution_id)
    institution_name_text = normalize_spaces(institution_name)
    trade_name_text = normalize_spaces(trade_name)
    cnpj_digits = digits_only(cnpj)

    return {
        "source_record_index": index,
        "institution_id": institution_id_text,
        "institution_name": institution_name_text,
        "institution_name_key": normalize_name_key(institution_name_text),
        "trade_name": trade_name_text,
        "trade_name_key": normalize_name_key(trade_name_text),
        "cnpj": cnpj_digits,
        "cnpj_root": cnpj_digits[:8] if cnpj_digits and len(cnpj_digits) >= 8 else None,
        "segment": normalize_spaces(segment),
        "conglomerate_name": normalize_spaces(conglomerate_name),
        "status_text": normalize_spaces(status_text),
        "is_active": infer_active(normalize_spaces(status_text)),
        "city": normalize_spaces(city),
        "uf": normalize_uf(uf),
        "start_date": normalize_spaces(start_date),
        "source_fields_present": sorted(record.keys()),
    }


def build_stage_payload(
    *,
    source_name: str,
    endpoint: str,
    records: List[Dict[str, Any]],
    page_meta: List[Dict[str, Any]],
    raw_hash: str,
    collected_at: str,
    mode_used: str,
) -> Dict[str, Any]:
    normalized_items = [normalize_record(record, index + 1) for index, record in enumerate(records)]
    normalized_items.sort(
        key=lambda item: (
            item.get("institution_id") or "",
            item.get("cnpj") or "",
            item.get("institution_name_key") or "",
        )
    )

    return {
        "metadata": {
            "source": source_name,
            "collector": "collectors/bc_cadastro_admins.py",
            "collected_at": collected_at,
            "endpoint": endpoint,
            "mode_used": mode_used,
            "record_count": len(normalized_items),
            "raw_hash": raw_hash,
            "page_count": len(page_meta),
        },
        "items": normalized_items,
    }


def read_existing_hash(stage_file: Path) -> Optional[str]:
    if not stage_file.exists():
        return None

    try:
        current = load_json(stage_file)
    except Exception:
        return None

    items = current.get("items")
    if not isinstance(items, list):
        return None

    return sha256_of(items)


def main() -> int:
    parser = argparse.ArgumentParser(description="Coletor do cadastro BC / SedesConsorcios.")
    parser.add_argument("--config", required=True, help="Caminho para config/sources.json")
    parser.add_argument("--source", default="bc_cadastro_admins", help="Nome da fonte em config/sources.json")
    args = parser.parse_args()

    config_path = Path(args.config)
    config = load_json(config_path)
    source_cfg = get_source_config(config, args.source)
    defaults = config.get("defaults", {})

    if not source_cfg.get("enabled", False):
        print(f"Fonte '{args.source}' está desabilitada.")
        append_github_output("changed", "false")
        append_github_output("records", "0")
        return 0

    api_cfg = source_cfg["api"]
    storage_cfg = source_cfg["storage"]

    endpoint = api_cfg["endpoint"]
    default_params = deepcopy(api_cfg.get("default_params", {}))
    pagination_param = api_cfg.get("pagination_param", "$skip")

    raw_dir = Path(storage_cfg["raw_dir"])
    stage_dir = Path(storage_cfg["stage_dir"])
    runtime_dir = Path("data/runtime")

    raw_file = raw_dir / "latest.json"
    stage_file = stage_dir / "instituicoes_cadastro.json"
    runtime_file = runtime_dir / "bc_cadastro_admins.json"

    raw_dir.mkdir(parents=True, exist_ok=True)
    stage_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    user_agent = defaults.get(
        "user_agent",
        "comparador-consorcios-data/1.0 (+https://sanida.com.br/financas/consorcio/)",
    )
    timeout_seconds = int(defaults.get("timeout_seconds", 60))

    headers = {
        "Accept": "application/json",
        "User-Agent": user_agent,
    }

    collected_at = utc_now_iso()
    session = build_session(timeout_seconds=timeout_seconds)

    mode_used = "unknown"
    try:
        records, page_meta, mode_used = fetch_paginated_odata_resilient(
            session=session,
            endpoint=endpoint,
            default_params=default_params,
            pagination_param=pagination_param,
            headers=headers,
        )
    except Exception:
        records, page_meta, mode_used = fetch_csv_fallback(
            session=session,
            endpoint=endpoint,
            headers=headers,
        )

    raw_payload = {
        "metadata": {
            "source": args.source,
            "collector": "collectors/bc_cadastro_admins.py",
            "collected_at": collected_at,
            "endpoint": endpoint,
            "mode_used": mode_used,
            "params": default_params,
            "page_count": len(page_meta),
            "record_count": len(records),
        },
        "pages": page_meta,
        "records": records,
    }

    raw_hash = sha256_of(records)
    stage_payload = build_stage_payload(
        source_name=args.source,
        endpoint=endpoint,
        records=records,
        page_meta=page_meta,
        raw_hash=raw_hash,
        collected_at=collected_at,
        mode_used=mode_used,
    )

    stage_items_hash = sha256_of(stage_payload["items"])
    previous_stage_hash = read_existing_hash(stage_file)
    changed = previous_stage_hash != stage_items_hash

    runtime_payload = {
        "source": args.source,
        "last_checked_at": collected_at,
        "last_changed_at": collected_at if changed else None,
        "changed": changed,
        "record_count": len(records),
        "raw_hash": raw_hash,
        "stage_items_hash": stage_items_hash,
        "mode_used": mode_used,
        "endpoint": endpoint,
        "raw_file": str(raw_file),
        "stage_file": str(stage_file),
    }

    if changed:
        dump_json(raw_file, raw_payload)
        dump_json(stage_file, stage_payload)

    dump_json(runtime_file, runtime_payload)

    append_github_output("changed", "true" if changed else "false")
    append_github_output("records", str(len(records)))
    append_github_output("stage_hash", stage_items_hash)

    print(
        json.dumps(
            {
                "source": args.source,
                "changed": changed,
                "records": len(records),
                "mode_used": mode_used,
                "raw_file": str(raw_file),
                "stage_file": str(stage_file),
                "runtime_file": str(runtime_file),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except requests.HTTPError as exc:
        print(f"Erro HTTP ao coletar cadastro BC: {exc}", file=sys.stderr)
        raise SystemExit(1)
    except Exception as exc:
        print(f"Falha no coletor bc_cadastro_admins: {exc}", file=sys.stderr)
        raise SystemExit(1)
