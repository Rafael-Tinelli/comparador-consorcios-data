#!/usr/bin/env python3
"""
Coletor das filiais / agências / postos de administradoras de consórcio via BCB Olinda em CSV.

Decisão desta implementação:
- usa exclusivamente o formato oficial CSV do recurso;
- não tenta OData JSON;
- não trata CSV como fallback;
- valida integridade mínima antes de aceitar a carga;
- bloqueia regressões fortes de volume e schema crítico.

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

    negative_markers = ["INATIV", "CANCEL", "SUSPENS", "ENCERR", "BAIXA", "EXTINT"]
    if any(marker in text for marker in negative_markers):
        return False

    positive_markers = ["ATIV", "AUTORIZ", "FUNCION", "REGULAR"]
    if any(marker in text for marker in positive_markers):
        return True

    return None


def detect_delimiter(text: str, configured: str) -> str:
    if configured and configured != "auto":
        return configured

    sample = text[:10000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
        return dialect.delimiter
    except Exception:
        if sample.count(";") > sample.count(","):
            return ";"
        return ","


def decode_csv_bytes(content: bytes, encoding_candidates: List[str]) -> Tuple[str, str]:
    last_error: Optional[Exception] = None
    for encoding in encoding_candidates:
        try:
            return content.decode(encoding), encoding
        except Exception as exc:
            last_error = exc

    if last_error:
        raise last_error
    raise RuntimeError("Não foi possível decodificar o CSV.")


def fetch_csv(
    session: requests.Session,
    endpoint: str,
    default_params: Dict[str, Any],
    headers: Dict[str, str],
    csv_cfg: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    params = deepcopy(default_params)
    params["$format"] = "text/csv"
    params.pop("$top", None)
    params.pop("$skip", None)

    headers_csv = dict(headers)
    headers_csv["Accept"] = "text/csv"

    response = session.get(
        endpoint,
        params=params,
        headers=headers_csv,
        timeout=getattr(session, "request_timeout", 60),
    )
    response.raise_for_status()

    text, encoding_used = decode_csv_bytes(
        response.content,
        csv_cfg.get("encoding_candidates", ["utf-8-sig", "utf-8", "latin-1", "cp1252"]),
    )
    text = text.lstrip("\ufeff")

    delimiter = detect_delimiter(text, csv_cfg.get("delimiter", "auto"))
    rows = list(csv.DictReader(io.StringIO(text), delimiter=delimiter))

    meta = {
        "mode": "odata-csv",
        "url": response.url,
        "status_code": response.status_code,
        "delimiter": delimiter,
        "encoding_used": encoding_used,
        "returned_records": len(rows),
        "response_headers": {
            "content_type": response.headers.get("Content-Type"),
            "content_length": response.headers.get("Content-Length"),
        },
    }
    return rows, meta


def normalize_record(record: Dict[str, Any], index: int) -> Dict[str, Any]:
    institution_id = pick_first(
        record,
        [
            "CodigoInstituicao",
            "CódigoInstituicao",
            "Codigo",
            "Código",
            "Codigo_IF",
            "CodInst",
            "CodIf",
            "ISPB"
        ],
    )
    institution_name = pick_first(
        record,
        [
            "NomeInstituicao",
            "Nome Instituicao",
            "Instituicao",
            "Instituição",
            "NomeIF",
            "Nome"
        ],
    )
    cnpj = pick_first(
        record,
        [
            "CNPJ",
            "Cnpj",
            "NumeroCNPJ",
            "NúmeroCNPJ",
            "CNPJCompleto"
        ],
    )
    unit_code = pick_first(
        record,
        [
            "CodigoDependencia",
            "CódigoDependencia",
            "CodigoFilial",
            "CódigoFilial",
            "CodDependencia",
            "CodFilial",
            "NumeroDependencia",
            "NúmeroDependencia",
            "CodigoAgencia",
            "CódigoAgencia"
        ],
    )
    unit_name = pick_first(
        record,
        [
            "NomeDependencia",
            "NomeDependência",
            "NomeFilial",
            "NomeAgencia",
            "NomeAgência",
            "NomePosto",
            "NomeUnidade"
        ],
    )
    unit_type = pick_first(
        record,
        [
            "TipoDependencia",
            "TipoDependência",
            "TipoFilial",
            "TipoAgencia",
            "TipoAgência",
            "TipoUnidade",
            "Categoria"
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
            "SituacaoDependencia",
            "SituaçãoDependência"
        ],
    )
    address = pick_first(
        record,
        [
            "Endereco",
            "Endereço",
            "Logradouro",
            "EnderecoCompleto",
            "EndereçoCompleto"
        ],
    )
    neighborhood = pick_first(
        record,
        [
            "Bairro",
            "Distrito"
        ],
    )
    city = pick_first(
        record,
        [
            "Municipio",
            "Município",
            "Cidade",
            "NomeMunicipio",
            "NomeMunicípio"
        ],
    )
    uf = pick_first(record, ["UF", "Uf", "SiglaUF", "SiglaUf"])
    zip_code = pick_first(
        record,
        [
            "CEP",
            "Cep",
            "CodigoPostal",
            "CódigoPostal"
        ],
    )
    phone = pick_first(
        record,
        [
            "Telefone",
            "Fone",
            "NumeroTelefone",
            "NúmeroTelefone",
            "DDDTelefone",
            "Contato"
        ],
    )
    start_date = pick_first(
        record,
        [
            "DataInicio",
            "DataInício",
            "DataAutorizacao",
            "DataAutorização",
            "InicioOperacao",
            "InícioOperação"
        ],
    )

    institution_id_text = normalize_spaces(institution_id)
    institution_name_text = normalize_spaces(institution_name)
    unit_code_text = normalize_spaces(unit_code)
    unit_name_text = normalize_spaces(unit_name)
    cnpj_digits = digits_only(cnpj)
    zip_code_digits = digits_only(zip_code)
    phone_digits = digits_only(phone)

    return {
        "source_record_index": index,
        "institution_id": institution_id_text,
        "institution_name": institution_name_text,
        "institution_name_key": normalize_name_key(institution_name_text),
        "cnpj": cnpj_digits,
        "cnpj_root": cnpj_digits[:8] if cnpj_digits and len(cnpj_digits) >= 8 else None,
        "unit_code": unit_code_text,
        "unit_name": unit_name_text,
        "unit_name_key": normalize_name_key(unit_name_text),
        "unit_type": normalize_spaces(unit_type),
        "status_text": normalize_spaces(status_text),
        "is_active": infer_active(normalize_spaces(status_text)),
        "address": normalize_spaces(address),
        "neighborhood": normalize_spaces(neighborhood),
        "city": normalize_spaces(city),
        "uf": normalize_uf(uf),
        "zip_code": zip_code_digits,
        "phone": phone_digits,
        "start_date": normalize_spaces(start_date),
        "source_fields_present": sorted(record.keys()),
    }


def count_previous_stage_items(stage_file: Path) -> Optional[int]:
    if not stage_file.exists():
        return None
    try:
        data = load_json(stage_file)
    except Exception:
        return None
    items = data.get("items")
    if not isinstance(items, list):
        return None
    return len(items)


def validate_row_volume(
    current_count: int,
    previous_count: Optional[int],
    expected_min_rows_absolute: int,
    expected_min_ratio_against_previous: float,
) -> None:
    if current_count < expected_min_rows_absolute:
        raise ValueError(
            f"Quantidade de registros insuficiente: {current_count} < {expected_min_rows_absolute}."
        )

    if previous_count and previous_count > 0:
        ratio = current_count / previous_count
        if ratio < expected_min_ratio_against_previous:
            raise ValueError(
                "Queda anormal de volume detectada: "
                f"{current_count} registros vs {previous_count} anteriores "
                f"(ratio={ratio:.4f} < {expected_min_ratio_against_previous:.4f})."
            )


def validate_required_normalized_fields(
    normalized_items: List[Dict[str, Any]],
    field_names: List[str],
    minimum_ratio: float,
) -> None:
    if not normalized_items:
        raise ValueError("Nenhum item normalizado foi gerado.")

    non_empty = 0
    for item in normalized_items:
        if any(item.get(field_name) for field_name in field_names):
            non_empty += 1

    ratio = non_empty / len(normalized_items)
    if ratio < minimum_ratio:
        raise ValueError(
            "Cobertura insuficiente dos campos normalizados críticos: "
            f"{ratio:.4f} < {minimum_ratio:.4f}."
        )


def build_stage_payload(
    *,
    source_name: str,
    endpoint: str,
    records: List[Dict[str, Any]],
    raw_hash: str,
    collected_at: str,
    mode_used: str,
    fetch_meta: Dict[str, Any],
) -> Dict[str, Any]:
    normalized_items = [normalize_record(record, index + 1) for index, record in enumerate(records)]
    normalized_items.sort(
        key=lambda item: (
            item.get("institution_id") or "",
            item.get("unit_code") or "",
            item.get("city") or "",
            item.get("uf") or "",
        )
    )

    return {
        "metadata": {
            "source": source_name,
            "collector": "collectors/bc_filiais.py",
            "collected_at": collected_at,
            "endpoint": endpoint,
            "mode_used": mode_used,
            "record_count": len(normalized_items),
            "raw_hash": raw_hash,
            "fetch_meta": fetch_meta
        },
        "items": normalized_items
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
    parser = argparse.ArgumentParser(description="Coletor do cadastro de filiais BC via CSV.")
    parser.add_argument("--config", required=True, help="Caminho para config/sources.json")
    parser.add_argument("--source", default="bc_filiais", help="Nome da fonte em config/sources.json")
    args = parser.parse_args()

    config_path = Path(args.config)
    config = load_json(config_path)
    source_cfg = get_source_config(config, args.source)
    defaults = config.get("defaults", {})
    csv_cfg = source_cfg.get("csv", {})

    if not source_cfg.get("enabled", False):
        print(f"Fonte '{args.source}' está desabilitada.")
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("mode_used", "disabled")
        return 0

    api_cfg = source_cfg["api"]
    storage_cfg = source_cfg["storage"]

    endpoint = api_cfg["endpoint"]
    default_params = deepcopy(api_cfg.get("default_params", {}))

    raw_dir = Path(storage_cfg["raw_dir"])
    stage_dir = Path(storage_cfg["stage_dir"])
    runtime_dir = Path("data/runtime")

    raw_file = raw_dir / "latest.json"
    stage_file = stage_dir / "filiais.json"
    runtime_file = runtime_dir / "bc_filiais.json"

    raw_dir.mkdir(parents=True, exist_ok=True)
    stage_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    user_agent = defaults.get(
        "user_agent",
        "comparador-consorcios-data/1.0 (+https://sanida.com.br/financas/consorcio/)",
    )
    timeout_seconds = int(defaults.get("timeout_seconds", 60))

    headers = {
        "Accept": "text/csv",
        "User-Agent": user_agent,
    }

    collected_at = utc_now_iso()
    session = build_session(timeout_seconds=timeout_seconds)

    records, fetch_meta = fetch_csv(
        session=session,
        endpoint=endpoint,
        default_params=default_params,
        headers=headers,
        csv_cfg=csv_cfg,
    )
    mode_used = "odata-csv"

    raw_hash = sha256_of(records)
    stage_payload = build_stage_payload(
        source_name=args.source,
        endpoint=endpoint,
        records=records,
        raw_hash=raw_hash,
        collected_at=collected_at,
        mode_used=mode_used,
        fetch_meta=fetch_meta,
    )

    normalized_items = stage_payload["items"]
    previous_count = count_previous_stage_items(stage_file)

    validate_row_volume(
        current_count=len(normalized_items),
        previous_count=previous_count,
        expected_min_rows_absolute=int(csv_cfg.get("expected_min_rows_absolute", 20)),
        expected_min_ratio_against_previous=float(csv_cfg.get("expected_min_ratio_against_previous", 0.85)),
    )

    validate_required_normalized_fields(
        normalized_items=normalized_items,
        field_names=csv_cfg.get(
            "required_normalized_fields_any",
            ["institution_id", "institution_name", "unit_code", "city", "uf"],
        ),
        minimum_ratio=float(csv_cfg.get("minimum_non_empty_required_field_ratio", 0.7)),
    )

    raw_payload = {
        "metadata": {
            "source": args.source,
            "collector": "collectors/bc_filiais.py",
            "collected_at": collected_at,
            "endpoint": endpoint,
            "mode_used": mode_used,
            "params": default_params,
            "record_count": len(records),
            "fetch_meta": fetch_meta,
        },
        "records": records,
    }

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
        "validation": {
            "expected_min_rows_absolute": int(csv_cfg.get("expected_min_rows_absolute", 20)),
            "expected_min_ratio_against_previous": float(csv_cfg.get("expected_min_ratio_against_previous", 0.85)),
            "previous_count": previous_count,
            "current_count": len(normalized_items),
        },
    }

    if changed:
        dump_json(raw_file, raw_payload)
        dump_json(stage_file, stage_payload)

    dump_json(runtime_file, runtime_payload)

    append_github_output("changed", "true" if changed else "false")
    append_github_output("records", str(len(records)))
    append_github_output("stage_hash", stage_items_hash)
    append_github_output("mode_used", mode_used)

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
        print(f"Erro HTTP ao coletar filiais BC em CSV: {exc}", file=sys.stderr)
        raise SystemExit(1)
    except Exception as exc:
        print(f"Falha no coletor bc_filiais: {exc}", file=sys.stderr)
        raise SystemExit(1)
