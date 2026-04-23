#!/usr/bin/env python3
"""
Coletor de séries SGS do BCB.

Ajustes principais desta versão:
- sempre envia dataInicial e dataFinal;
- quebra consultas longas em janelas menores;
- falha de forma explícita quando o endpoint responder HTML/erro em vez de JSON;
- grava raw, stage e runtime;
- só considera sucesso quando mode_used = "sgs-json-range".
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


MODE_USED = "sgs-json-range"
DEFAULT_MAX_DAYS_PER_REQUEST = 3650


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def today_utc() -> date:
    return datetime.now(timezone.utc).date()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def dump_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2, sort_keys=False)
        fh.write("\n")


def stable_json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_data(data: Any) -> str:
    return sha256_text(stable_json_dumps(data))


def append_github_output(name: str, value: str) -> None:
    github_output = os.environ.get("GITHUB_OUTPUT")
    if not github_output:
        return
    with open(github_output, "a", encoding="utf-8") as fh:
        fh.write(f"{name}={value}\n")


def normalize_spaces(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    return text or None


def parse_br_date(value: str) -> date:
    return datetime.strptime(value, "%d/%m/%Y").date()


def format_br_date(value: date) -> str:
    return value.strftime("%d/%m/%Y")


def format_iso_date(value: date) -> str:
    return value.isoformat()


def parse_float_br(value: Any) -> Optional[float]:
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


def get_source_config(config: Dict[str, Any], source_name: str) -> Dict[str, Any]:
    try:
        return config["sources"][source_name]
    except KeyError as exc:
        raise KeyError(f"Fonte '{source_name}' não encontrada em config/sources.json.") from exc


def load_series_map(path: Path) -> Dict[str, Any]:
    payload = load_json(path)
    if not isinstance(payload, dict):
        raise ValueError("config/series_map.json inválido: esperado objeto JSON.")
    return payload


def iter_enabled_series(series_map: Dict[str, Any]) -> List[Dict[str, Any]]:
    groups = series_map.get("groups", {})
    if not isinstance(groups, dict):
        raise ValueError("config/series_map.json inválido: campo 'groups' ausente ou inválido.")

    enabled_items: List[Dict[str, Any]] = []

    for group_name, items in groups.items():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            if not item.get("enabled", False):
                continue
            serie = item.get("serie")
            if serie in (None, "", 0):
                continue
            enabled_items.append(
                {
                    "group": group_name,
                    "key": item.get("key"),
                    "label": item.get("label"),
                    "source": item.get("source"),
                    "serie": int(serie),
                    "unit": item.get("unit"),
                    "frequency": item.get("frequency"),
                    "category": item.get("category"),
                    "notes": item.get("notes"),
                    "data_inicial": item.get("data_inicial"),
                }
            )

    if not enabled_items:
        raise ValueError("Nenhuma série habilitada com código SGS válido em config/series_map.json.")

    return enabled_items


def build_date_chunks(start_date: date, end_date: date, max_days: int) -> List[Tuple[date, date]]:
    if end_date < start_date:
        raise ValueError(f"Intervalo inválido: data_final {end_date} menor que data_inicial {start_date}.")
    if max_days < 1:
        raise ValueError("max_days deve ser >= 1.")

    chunks: List[Tuple[date, date]] = []
    current_start = start_date

    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=max_days - 1), end_date)
        chunks.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)

    return chunks


def preview_text(text: str, limit: int = 320) -> str:
    return " ".join((text or "").split())[:limit]


def fetch_series_chunk(
    *,
    session: requests.Session,
    endpoint_template: str,
    serie: int,
    default_params: Dict[str, Any],
    start_date: date,
    end_date: date,
    user_agent: str,
) -> Dict[str, Any]:
    url = endpoint_template.format(serie=serie)
    params = dict(default_params or {})
    params["formato"] = params.get("formato") or "json"
    params["dataInicial"] = format_br_date(start_date)
    params["dataFinal"] = format_br_date(end_date)

    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": user_agent,
    }

    response = session.get(
        url,
        params=params,
        headers=headers,
        timeout=getattr(session, "request_timeout", 60),
        allow_redirects=True,
    )
    response.raise_for_status()

    text = response.text or ""
    content_type = (response.headers.get("Content-Type") or "").lower()

    try:
        payload = response.json()
    except Exception as exc:
        raise ValueError(
            "Série retornou corpo não-JSON. "
            f"status={response.status_code} "
            f"content_type={response.headers.get('Content-Type')!r} "
            f"url={response.url} "
            f"preview={preview_text(text)!r}"
        ) from exc

    if not isinstance(payload, list):
        raise ValueError(
            "Série retornou JSON em formato inesperado. "
            f"status={response.status_code} "
            f"content_type={response.headers.get('Content-Type')!r} "
            f"url={response.url} "
            f"payload_type={type(payload).__name__}"
        )

    return {
        "request": {
            "url": response.url,
            "status_code": response.status_code,
            "content_type": response.headers.get("Content-Type"),
            "requested_start": format_br_date(start_date),
            "requested_end": format_br_date(end_date),
        },
        "payload": payload,
    }


def normalize_series_points(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    points_by_iso: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue

        raw_date = row.get("data")
        raw_value = row.get("valor")

        if raw_date in (None, ""):
            continue

        try:
            parsed_date = parse_br_date(str(raw_date))
        except Exception:
            continue

        iso_date = format_iso_date(parsed_date)
        value = parse_float_br(raw_value)

        points_by_iso[iso_date] = {
            "date": iso_date,
            "data": format_br_date(parsed_date),
            "value": value,
            "raw_value": raw_value,
        }

    return [points_by_iso[key] for key in sorted(points_by_iso.keys())]


def build_series_entry(series_def: Dict[str, Any], all_rows: List[Dict[str, Any]], requests_meta: List[Dict[str, Any]]) -> Dict[str, Any]:
    values = normalize_series_points(all_rows)

    latest_value = None
    latest_date = None
    latest_date_br = None

    if values:
        last = values[-1]
        latest_value = last.get("value")
        latest_date = last.get("date")
        latest_date_br = last.get("data")

    first_date = values[0]["date"] if values else None
    first_date_br = values[0]["data"] if values else None

    return {
        "group": series_def["group"],
        "key": series_def["key"],
        "label": series_def["label"],
        "source": series_def.get("source"),
        "serie": series_def["serie"],
        "unit": series_def.get("unit"),
        "frequency": series_def.get("frequency"),
        "category": series_def.get("category"),
        "notes": series_def.get("notes"),
        "data_inicial_consulta": requests_meta[0]["requested_start"] if requests_meta else None,
        "data_final_consulta": requests_meta[-1]["requested_end"] if requests_meta else None,
        "points": len(values),
        "first_date": first_date,
        "first_date_br": first_date_br,
        "latest_date": latest_date,
        "latest_date_br": latest_date_br,
        "latest_value": latest_value,
        "values": values,
        "requests": requests_meta,
    }


def build_fingerprint(series_entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    compact = []
    for item in series_entries:
        compact.append(
            {
                "key": item.get("key"),
                "serie": item.get("serie"),
                "points": item.get("points"),
                "first_date": item.get("first_date"),
                "latest_date": item.get("latest_date"),
                "latest_value": item.get("latest_value"),
                "values": item.get("values", []),
            }
        )
    return {"series": compact}


def previous_content_hash(stage_file: Path) -> Optional[str]:
    if not stage_file.exists():
        return None
    try:
        payload = load_json(stage_file)
        if isinstance(payload, dict):
            metadata = payload.get("metadata") or {}
            existing_hash = metadata.get("content_hash")
            if existing_hash:
                return str(existing_hash)

            series_entries = payload.get("series") or []
            if isinstance(series_entries, list):
                return sha256_data(build_fingerprint(series_entries))
    except Exception:
        return None
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Coletor de séries SGS do BCB.")
    parser.add_argument("--config", required=True, help="Caminho para config/sources.json")
    parser.add_argument("--source", default="bc_sgs_series", help="Nome da fonte em config/sources.json")
    args = parser.parse_args()

    config = load_json(Path(args.config))
    source_cfg = get_source_config(config, args.source)
    defaults = config.get("defaults", {})

    raw_dir = Path(source_cfg["storage"]["raw_dir"])
    stage_dir = Path(source_cfg["storage"]["stage_dir"])
    runtime_dir = Path("data/runtime")
    series_map_file = Path(source_cfg.get("series_map_file", "config/series_map.json"))

    raw_file = raw_dir / "latest.json"
    stage_file = stage_dir / "series.json"
    runtime_file = runtime_dir / "bc_sgs_series.json"

    raw_dir.mkdir(parents=True, exist_ok=True)
    stage_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    collected_at = utc_now_iso()

    if not source_cfg.get("enabled", False):
        runtime_payload = {
            "source": args.source,
            "last_checked_at": collected_at,
            "changed": False,
            "mode_used": "disabled",
            "raw_file": str(raw_file),
            "stage_file": str(stage_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("mode_used", "disabled")
        print(f"Fonte '{args.source}' está desabilitada.")
        return 0

    session = build_session(timeout_seconds=int(defaults.get("timeout_seconds", 60)))
    user_agent = defaults.get(
        "user_agent",
        "comparador-consorcios-data/1.0 (+https://sanida.com.br/financas/consorcio/)",
    )

    api_cfg = source_cfg.get("api", {})
    endpoint_template = api_cfg.get("endpoint_template")
    default_params = dict(api_cfg.get("default_params", {}))

    if not endpoint_template:
        raise ValueError("config/sources.json: bc_sgs_series.api.endpoint_template ausente.")

    max_days = int(api_cfg.get("max_days_per_request", DEFAULT_MAX_DAYS_PER_REQUEST))
    if max_days < 1:
        max_days = DEFAULT_MAX_DAYS_PER_REQUEST

    series_map = load_series_map(series_map_file)
    series_defs = iter_enabled_series(series_map)

    series_entries: List[Dict[str, Any]] = []
    request_log: List[Dict[str, Any]] = []
    point_count_total = 0

    try:
        for series_def in series_defs:
            start_value = normalize_spaces(series_def.get("data_inicial")) or "01/01/2017"
            end_date = today_utc()
            start_date = parse_br_date(start_value)

            chunks = build_date_chunks(start_date, end_date, max_days=max_days)
            all_rows: List[Dict[str, Any]] = []
            requests_meta: List[Dict[str, Any]] = []

            for chunk_start, chunk_end in chunks:
                result = fetch_series_chunk(
                    session=session,
                    endpoint_template=endpoint_template,
                    serie=series_def["serie"],
                    default_params=default_params,
                    start_date=chunk_start,
                    end_date=chunk_end,
                    user_agent=user_agent,
                )
                payload_rows = result["payload"]
                requests_meta.append(result["request"])
                request_log.append(
                    {
                        "key": series_def["key"],
                        "serie": series_def["serie"],
                        **result["request"],
                        "rows_returned": len(payload_rows),
                    }
                )
                all_rows.extend(payload_rows)

            entry = build_series_entry(series_def, all_rows, requests_meta)
            point_count_total += entry["points"]
            series_entries.append(entry)

        fingerprint = build_fingerprint(series_entries)
        content_hash = sha256_data(fingerprint)
        prev_hash = previous_content_hash(stage_file)
        changed = content_hash != prev_hash

        raw_payload = {
            "metadata": {
                "source": args.source,
                "collector": "collectors/bc_sgs_series.py",
                "collected_at": collected_at,
                "mode_used": MODE_USED,
                "series_map_file": str(series_map_file),
                "series_count": len(series_entries),
                "point_count_total": point_count_total,
                "content_hash": content_hash,
            },
            "request_log": request_log,
            "series": series_entries,
        }

        stage_payload = {
            "metadata": {
                "source": args.source,
                "collector": "collectors/bc_sgs_series.py",
                "collected_at": collected_at,
                "mode_used": MODE_USED,
                "series_count": len(series_entries),
                "point_count_total": point_count_total,
                "content_hash": content_hash,
            },
            "series": series_entries,
        }

        runtime_payload = {
            "source": args.source,
            "last_checked_at": collected_at,
            "last_changed_at": collected_at if changed else None,
            "changed": changed,
            "mode_used": MODE_USED,
            "series_count": len(series_entries),
            "point_count_total": point_count_total,
            "content_hash": content_hash,
            "raw_file": str(raw_file),
            "stage_file": str(stage_file),
        }

        dump_json(raw_file, raw_payload)
        dump_json(stage_file, stage_payload)
        dump_json(runtime_file, runtime_payload)

        append_github_output("changed", "true" if changed else "false")
        append_github_output("records", str(point_count_total))
        append_github_output("stage_hash", content_hash)
        append_github_output("mode_used", MODE_USED)

        print(
            json.dumps(
                {
                    "source": args.source,
                    "changed": changed,
                    "series_count": len(series_entries),
                    "records": point_count_total,
                    "mode_used": MODE_USED,
                    "raw_file": str(raw_file),
                    "stage_file": str(stage_file),
                    "runtime_file": str(runtime_file),
                },
                ensure_ascii=False,
            )
        )
        return 0

    except requests.HTTPError as exc:
        runtime_payload = {
            "source": args.source,
            "last_checked_at": collected_at,
            "changed": False,
            "mode_used": "sgs-json-range-failed",
            "error": f"Erro HTTP ao coletar séries SGS: {exc}",
            "raw_file": str(raw_file),
            "stage_file": str(stage_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("mode_used", "sgs-json-range-failed")
        print(f"Erro HTTP ao coletar séries SGS: {exc}", file=sys.stderr)
        return 1

    except Exception as exc:
        runtime_payload = {
            "source": args.source,
            "last_checked_at": collected_at,
            "changed": False,
            "mode_used": "sgs-json-range-failed",
            "error": str(exc),
            "raw_file": str(raw_file),
            "stage_file": str(stage_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("mode_used", "sgs-json-range-failed")
        print(f"Falha no coletor bc_sgs_series: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
