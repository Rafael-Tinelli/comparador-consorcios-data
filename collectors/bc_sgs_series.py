#!/usr/bin/env python3
"""
Coletor de séries do Banco Central via SGS JSON.

Objetivos desta versão:
- usar exclusivamente o endpoint oficial SGS em JSON;
- falhar de forma transparente, informando qual série devolveu corpo inválido;
- não mascarar fallback;
- gerar artefatos em raw, stage e runtime;
- só considerar sucesso quando mode_used = "sgs-json".

Este coletor não gera data/dist. Isso fica para o build-read-models.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from copy import deepcopy
from datetime import datetime, timezone
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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


def normalize_spaces(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    text = " ".join(text.split())
    return text or None


def parse_sgs_date(value: str) -> str:
    text = str(value).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"Data SGS inválida: {value!r}")


def parse_sgs_value(value: Any) -> float:
    if value is None:
        raise ValueError("Valor SGS ausente.")
    text = str(value).strip()
    if not text:
        raise ValueError("Valor SGS vazio.")

    # 1.234,56 -> 1234.56
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", ".")

    return float(text)


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


def iter_series_entries(series_map: Dict[str, Any]) -> Iterable[Tuple[str, Dict[str, Any]]]:
    groups = series_map.get("groups", {})
    if isinstance(groups, dict):
        for group_name, items in groups.items():
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        yield group_name, item

    top_level_series = series_map.get("series")
    if isinstance(top_level_series, list):
        for item in top_level_series:
            if isinstance(item, dict):
                yield "default", item


def collect_enabled_series(series_map: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
    collected: List[Dict[str, Any]] = []
    errors: List[str] = []

    for group_name, item in iter_series_entries(series_map):
        enabled = bool(item.get("enabled", False))
        if not enabled:
            continue

        key = normalize_spaces(item.get("key"))
        label = normalize_spaces(item.get("label"))
        serie = item.get("serie")

        if not key:
            errors.append(f"Grupo '{group_name}': item habilitado sem 'key'.")
            continue

        if serie in (None, ""):
            errors.append(f"Série '{key}' habilitada sem código 'serie'.")
            continue

        collected.append(
            {
                "group": group_name,
                "key": key,
                "label": label or key,
                "serie": str(serie).strip(),
                "source": normalize_spaces(item.get("source")) or "BCB/SGS",
                "unit": normalize_spaces(item.get("unit")),
                "frequency": normalize_spaces(item.get("frequency")),
                "category": normalize_spaces(item.get("category")),
                "notes": normalize_spaces(item.get("notes")),
                "data_inicial": normalize_spaces(item.get("dataInicial") or item.get("data_inicial") or item.get("start_date")),
                "data_final": normalize_spaces(item.get("dataFinal") or item.get("data_final") or item.get("end_date")),
            }
        )

    collected.sort(key=lambda x: (x["group"], x["key"]))
    return collected, errors


def build_request_params(default_params: Dict[str, Any], series_cfg: Dict[str, Any]) -> Dict[str, Any]:
    params = {}
    for key, value in default_params.items():
        if value is not None:
            params[key] = value

    if series_cfg.get("data_inicial"):
        params["dataInicial"] = series_cfg["data_inicial"]
    if series_cfg.get("data_final"):
        params["dataFinal"] = series_cfg["data_final"]

    params["formato"] = "json"
    return params


def preview_text(text: str, limit: int = 300) -> str:
    compact = " ".join(text.split())
    return compact[:limit]


def decode_json_response(response: requests.Response, series_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    text = response.text or ""
    stripped = text.strip()

    if not stripped:
        raise ValueError(
            f"Série '{series_cfg['key']}' ({series_cfg['serie']}) retornou corpo vazio. "
            f"status={response.status_code} content_type={response.headers.get('Content-Type')!r} url={response.url}"
        )

    try:
        payload = response.json()
    except JSONDecodeError:
        raise ValueError(
            f"Série '{series_cfg['key']}' ({series_cfg['serie']}) retornou corpo não-JSON. "
            f"status={response.status_code} content_type={response.headers.get('Content-Type')!r} "
            f"url={response.url} preview={preview_text(stripped)!r}"
        )

    if not isinstance(payload, list):
        raise ValueError(
            f"Série '{series_cfg['key']}' ({series_cfg['serie']}) retornou payload inesperado; "
            f"esperado list, recebido {type(payload).__name__}. "
            f"status={response.status_code} content_type={response.headers.get('Content-Type')!r} url={response.url}"
        )

    return payload


def fetch_sgs_series(
    session: requests.Session,
    endpoint_template: str,
    default_params: Dict[str, Any],
    headers: Dict[str, str],
    series_cfg: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    endpoint = endpoint_template.format(serie=series_cfg["serie"])
    params = build_request_params(default_params, series_cfg)

    response = session.get(
        endpoint,
        params=params,
        headers=headers,
        timeout=getattr(session, "request_timeout", 60),
    )
    response.raise_for_status()

    payload = decode_json_response(response, series_cfg)

    normalized_points: List[Dict[str, Any]] = []
    dropped_points = 0

    for row in payload:
        if not isinstance(row, dict):
            dropped_points += 1
            continue

        raw_date = row.get("data")
        raw_value = row.get("valor")

        try:
            point = {
                "date": parse_sgs_date(str(raw_date)),
                "value": parse_sgs_value(raw_value),
                "raw_date": raw_date,
                "raw_value": raw_value,
            }
            normalized_points.append(point)
        except Exception:
            dropped_points += 1

    normalized_points.sort(key=lambda x: x["date"])

    if not normalized_points:
        raise ValueError(f"A série '{series_cfg['key']}' ({series_cfg['serie']}) não retornou pontos válidos.")

    fetch_meta = {
        "url": response.url,
        "status_code": response.status_code,
        "content_type": response.headers.get("Content-Type"),
        "returned_points_raw": len(payload),
        "returned_points_valid": len(normalized_points),
        "dropped_points": dropped_points,
        "first_date": normalized_points[0]["date"],
        "last_date": normalized_points[-1]["date"],
    }
    return normalized_points, fetch_meta


def build_stats(points: List[Dict[str, Any]]) -> Dict[str, Any]:
    values = [point["value"] for point in points]
    return {
        "count": len(points),
        "first_date": points[0]["date"],
        "last_date": points[-1]["date"],
        "min_value": min(values),
        "max_value": max(values),
        "last_value": points[-1]["value"],
    }


def build_stage_item(series_cfg: Dict[str, Any], points: List[Dict[str, Any]], fetch_meta: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "group": series_cfg["group"],
        "key": series_cfg["key"],
        "label": series_cfg["label"],
        "source": series_cfg["source"],
        "serie": series_cfg["serie"],
        "unit": series_cfg["unit"],
        "frequency": series_cfg["frequency"],
        "category": series_cfg["category"],
        "notes": series_cfg["notes"],
        "stats": build_stats(points),
        "points": points,
        "fetch_meta": fetch_meta,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Coletor de séries SGS do Banco Central.")
    parser.add_argument("--config", required=True, help="Caminho para config/sources.json")
    parser.add_argument("--source", default="bc_sgs_series", help="Nome da fonte em config/sources.json")
    args = parser.parse_args()

    config_path = Path(args.config)
    config = load_json(config_path)
    source_cfg = get_source_config(config, args.source)
    defaults = config.get("defaults", {})

    if not source_cfg.get("enabled", False):
        print(f"Fonte '{args.source}' está desabilitada.")
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("series_count", "0")
        append_github_output("mode_used", "disabled")
        return 0

    api_cfg = source_cfg["api"]
    storage_cfg = source_cfg["storage"]
    series_map_file = Path(source_cfg["series_map_file"])

    if not series_map_file.exists():
        raise FileNotFoundError(f"Arquivo de mapeamento de séries não encontrado: {series_map_file}")

    series_map = load_json(series_map_file)
    enabled_series, config_errors = collect_enabled_series(series_map)

    if config_errors:
        raise ValueError("Erros de configuração em series_map.json: " + " | ".join(config_errors))

    if not enabled_series:
        raise ValueError("Nenhuma série válida e habilitada encontrada em config/series_map.json.")

    endpoint_template = api_cfg["endpoint_template"]
    default_params = deepcopy(api_cfg.get("default_params", {}))

    raw_dir = Path(storage_cfg["raw_dir"])
    stage_dir = Path(storage_cfg["stage_dir"])
    runtime_dir = Path("data/runtime")

    raw_file = raw_dir / "latest.json"
    stage_file = stage_dir / "series.json"
    runtime_file = runtime_dir / "bc_sgs_series.json"

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

    raw_series_items: List[Dict[str, Any]] = []
    stage_items: List[Dict[str, Any]] = []
    total_points = 0

    for series_cfg in enabled_series:
        points, fetch_meta = fetch_sgs_series(
            session=session,
            endpoint_template=endpoint_template,
            default_params=default_params,
            headers=headers,
            series_cfg=series_cfg,
        )

        raw_series_items.append(
            {
                "series_config": series_cfg,
                "fetch_meta": fetch_meta,
                "points": points,
            }
        )
        stage_items.append(build_stage_item(series_cfg, points, fetch_meta))
        total_points += len(points)

    stage_items.sort(key=lambda x: (x["group"], x["key"]))

    raw_hash = sha256_of(raw_series_items)
    raw_payload = {
        "metadata": {
            "source": args.source,
            "collector": "collectors/bc_sgs_series.py",
            "collected_at": collected_at,
            "mode_used": "sgs-json",
            "series_count": len(stage_items),
            "total_points": total_points,
            "series_map_file": str(series_map_file),
            "raw_hash": raw_hash,
        },
        "series": raw_series_items,
    }

    stage_payload = {
        "metadata": {
            "source": args.source,
            "collector": "collectors/bc_sgs_series.py",
            "collected_at": collected_at,
            "mode_used": "sgs-json",
            "series_count": len(stage_items),
            "total_points": total_points,
            "series_map_file": str(series_map_file),
            "raw_hash": raw_hash,
        },
        "items": stage_items,
    }

    stage_items_hash = sha256_of(stage_payload["items"])
    previous_stage_hash = read_existing_hash(stage_file)
    changed = previous_stage_hash != stage_items_hash

    runtime_payload = {
        "source": args.source,
        "last_checked_at": collected_at,
        "last_changed_at": collected_at if changed else None,
        "changed": changed,
        "series_count": len(stage_items),
        "total_points": total_points,
        "raw_hash": raw_hash,
        "stage_items_hash": stage_items_hash,
        "mode_used": "sgs-json",
        "series_map_file": str(series_map_file),
        "keys": [item["key"] for item in stage_items],
        "raw_file": str(raw_file),
        "stage_file": str(stage_file),
    }

    if changed:
        dump_json(raw_file, raw_payload)
        dump_json(stage_file, stage_payload)

    dump_json(runtime_file, runtime_payload)

    append_github_output("changed", "true" if changed else "false")
    append_github_output("records", str(total_points))
    append_github_output("series_count", str(len(stage_items)))
    append_github_output("stage_hash", stage_items_hash)
    append_github_output("mode_used", "sgs-json")

    print(
        json.dumps(
            {
                "source": args.source,
                "changed": changed,
                "series_count": len(stage_items),
                "records": total_points,
                "mode_used": "sgs-json",
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
        print(f"Erro HTTP ao coletar séries SGS: {exc}", file=sys.stderr)
        raise SystemExit(1)
    except Exception as exc:
        print(f"Falha no coletor bc_sgs_series: {exc}", file=sys.stderr)
        raise SystemExit(1)
