#!/usr/bin/env python3
"""
Coletor de séries SGS do BCB.

Estratégia desta versão:
- usa o endpoint de "últimos N valores" do SGS;
- aplica clamp de N em 20, conforme limite documentado;
- mantém mode_used = "sgs-json-range" por compatibilidade com o workflow 03 já instalado;
- grava raw, stage e runtime;
- falha de forma explícita quando o endpoint responder HTML/erro em vez de JSON.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Mantido por compatibilidade com o workflow 03 já existente.
MODE_USED = "sgs-json-range"

# O endpoint "ultimos/N" do SGS é limitado a 20.
MAX_LATEST_POINTS = 20

DEFAULT_POINTS_BY_FREQUENCY = {
    "daily": 20,
    "monthly": 20,
    "quarterly": 20,
    "weekly": 20,
    "yearly": 20,
    "annual": 20,
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


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


def normalize_key(value: Optional[str]) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


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
                }
            )

    if not enabled_items:
        raise ValueError("Nenhuma série habilitada com código SGS válido em config/series_map.json.")

    return enabled_items


def preview_text(text: str, limit: int = 320) -> str:
    return " ".join((text or "").split())[:limit]


def clamp_points(points: int) -> int:
    if points < 1:
        return 1
    if points > MAX_LATEST_POINTS:
        return MAX_LATEST_POINTS
    return points


def points_to_request(series_def: Dict[str, Any], api_cfg: Dict[str, Any]) -> int:
    override_by_key = api_cfg.get("latest_points_by_key", {})
    if isinstance(override_by_key, dict):
        value = override_by_key.get(series_def["key"])
        if value is not None:
            try:
                return clamp_points(int(value))
            except Exception:
                pass

    override_by_frequency = api_cfg.get("latest_points_by_frequency", {})
    frequency = normalize_key(series_def.get("frequency")) or "default"

    if isinstance(override_by_frequency, dict):
        value = override_by_frequency.get(frequency)
        if value is not None:
            try:
                return clamp_points(int(value))
            except Exception:
                pass

    default_value = DEFAULT_POINTS_BY_FREQUENCY.get(frequency, api_cfg.get("latest_points_default", 12))
    try:
        return clamp_points(int(default_value))
    except Exception:
        return 12


def fetch_series_latest(
    *,
    session: requests.Session,
    latest_endpoint_template: str,
    serie: int,
    points: int,
    default_params: Dict[str, Any],
    user_agent: str,
) -> Dict[str, Any]:
    url = latest_endpoint_template.format(serie=serie, ultimos=points)
    params = dict(default_params or {})
    params["formato"] = params.get("formato") or "json"

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
            "requested_points": points,
        },
        "payload": payload,
    }


def normalize_series_points(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    points_by_date: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue

        raw_date = row.get("data")
        raw_value = row.get("valor")

        if raw_date in (None, ""):
            continue

        raw_date_str = str(raw_date).strip()
        value = parse_float_br(raw_value)

        points_by_date[raw_date_str] = {
            "date": raw_date_str,
            "data": raw_date_str,
            "value": value,
            "raw_value": raw_value,
        }

    def sort_key(item: Dict[str, Any]) -> tuple:
        raw = item["date"]
        try:
            dt = datetime.strptime(raw, "%d/%m/%Y")
            return (0, dt)
        except Exception:
            try:
                dt = datetime.strptime(raw, "%Y-%m-%d")
                return (0, dt)
            except Exception:
                return (1, raw)

    values = list(points_by_date.values())
    values.sort(key=sort_key)
    return values


def build_series_entry(
    series_def: Dict[str, Any],
    rows: List[Dict[str, Any]],
    request_meta: Dict[str, Any],
    points_requested: int,
) -> Dict[str, Any]:
    values = normalize_series_points(rows)

    latest_value = None
    latest_date = None
    first_date = None

    if values:
        first_date = values[0].get("date")
        latest_date = values[-1].get("date")
        latest_value = values[-1].get("value")

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
        "collection_strategy": "latest_n",
        "requested_points": points_requested,
        "points": len(values),
        "first_date": first_date,
        "latest_date": latest_date,
        "latest_value": latest_value,
        "values": values,
        "request": request_meta,
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
    default_params = dict(api_cfg.get("default_params", {}))

    latest_endpoint_template = api_cfg.get(
        "latest_endpoint_template",
        "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados/ultimos/{ultimos}",
    )

    series_map = load_series_map(series_map_file)
    series_defs = iter_enabled_series(series_map)

    series_entries: List[Dict[str, Any]] = []
    request_log: List[Dict[str, Any]] = []
    point_count_total = 0

    try:
        for series_def in series_defs:
            points_requested = points_to_request(series_def, api_cfg)

            result = fetch_series_latest(
                session=session,
                latest_endpoint_template=latest_endpoint_template,
                serie=series_def["serie"],
                points=points_requested,
                default_params=default_params,
                user_agent=user_agent,
            )

            payload_rows = result["payload"]
            request_meta = result["request"]

            request_log.append(
                {
                    "key": series_def["key"],
                    "serie": series_def["serie"],
                    **request_meta,
                    "rows_returned": len(payload_rows),
                }
            )

            entry = build_series_entry(
                series_def=series_def,
                rows=payload_rows,
                request_meta=request_meta,
                points_requested=points_requested,
            )
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
            "mode_used": "sgs-json-latest-failed",
            "error": f"Erro HTTP ao coletar séries SGS: {exc}",
            "raw_file": str(raw_file),
            "stage_file": str(stage_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("mode_used", "sgs-json-latest-failed")
        print(f"Erro HTTP ao coletar séries SGS: {exc}", file=sys.stderr)
        return 1

    except Exception as exc:
        runtime_payload = {
            "source": args.source,
            "last_checked_at": collected_at,
            "changed": False,
            "mode_used": "sgs-json-latest-failed",
            "error": str(exc),
            "raw_file": str(raw_file),
            "stage_file": str(stage_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("mode_used", "sgs-json-latest-failed")
        print(f"Falha no coletor bc_sgs_series: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
