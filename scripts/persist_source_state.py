#!/usr/bin/env python3
"""Persist durable source check/change/competence state for Comparador V2.

Collector runtime is intentionally ephemeral. This script converts a collector attempt
into a small versioned state document that survives across GitHub Actions runs without
rewriting the underlying dataset when content is unchanged.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

SCHEMA = "source-state.v1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def load_json_if_exists(path: Optional[Path]) -> Any:
    if path is None or not path.exists():
        return None
    try:
        return load_json(path)
    except Exception:
        return None


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def sha256_file(path: Optional[Path]) -> Optional[str]:
    if path is None or not path.exists() or not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def nested(payload: Any, keys: Iterable[str]) -> Any:
    cur = payload
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def normalized_date(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", text)
    if match:
        return f"{match.group(3)}-{match.group(2)}-{match.group(1)}"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    return None


def infer_competence(runtime: Any, snapshot: Any, mode: str) -> Dict[str, Any]:
    if mode == "not_applicable":
        return {"kind": "not_applicable", "value": None}

    if isinstance(runtime, dict):
        value = runtime.get("competencia") or runtime.get("yyyymm")
        if isinstance(value, str) and re.fullmatch(r"20\d{2}(0[1-9]|1[0-2])", value):
            return {"kind": "month", "value": value}

    if isinstance(snapshot, dict):
        value = nested(snapshot, ("selected_resource", "selected_candidate", "yyyymm"))
        if isinstance(value, str) and re.fullmatch(r"20\d{2}(0[1-9]|1[0-2])", value):
            return {"kind": "month", "value": value}

        selected = snapshot.get("selected")
        if isinstance(selected, dict):
            year = selected.get("ano")
            period = selected.get("periodo")
            periodicity = str(selected.get("periodicidade") or "").upper()
            if isinstance(year, int) and isinstance(period, int):
                if "SEMEST" in periodicity and period in (1, 2):
                    return {
                        "kind": "semester",
                        "value": f"{year}-S{period}",
                        "year": year,
                        "period": period,
                    }
                return {
                    "kind": "period",
                    "value": f"{year}-{period}",
                    "year": year,
                    "period": period,
                }

        records = snapshot.get("records")
        if isinstance(records, list):
            positions = set()
            for row in records[:500]:
                if not isinstance(row, dict):
                    continue
                for key in ("Posicao", "Posição", "posicao", "position_date"):
                    if key in row:
                        date_value = normalized_date(row.get(key))
                        if date_value:
                            positions.add(date_value)
            if len(positions) == 1:
                return {"kind": "position_date", "value": next(iter(positions))}

    return {"kind": "not_reported", "value": None}


def snapshot_collected_at(snapshot: Any) -> Optional[str]:
    value = nested(snapshot, ("metadata", "collected_at"))
    return str(value) if value else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Persiste estado durável de uma fonte do Comparador V2")
    parser.add_argument("--source", required=True)
    parser.add_argument("--runtime")
    parser.add_argument("--snapshot")
    parser.add_argument("--content-file")
    parser.add_argument("--state", required=True)
    parser.add_argument("--collector-outcome", default="success")
    parser.add_argument("--accepted-mode", action="append", default=[])
    parser.add_argument("--competence-mode", choices=("auto", "not_applicable"), default="auto")
    parser.add_argument("--error")
    args = parser.parse_args()

    runtime_path = Path(args.runtime) if args.runtime else None
    snapshot_path = Path(args.snapshot) if args.snapshot else None
    content_path = Path(args.content_file) if args.content_file else None
    state_path = Path(args.state)

    previous = load_json_if_exists(state_path)
    if not isinstance(previous, dict) or previous.get("schema") != SCHEMA:
        previous = {}

    runtime = load_json_if_exists(runtime_path)
    snapshot = load_json_if_exists(snapshot_path)

    runtime_mode = runtime.get("mode_used") if isinstance(runtime, dict) else None
    if not runtime_mode and isinstance(snapshot, dict):
        runtime_mode = nested(snapshot, ("metadata", "mode_used"))
    accepted_mode = not args.accepted_mode or runtime_mode in set(args.accepted_mode)
    collector_succeeded = args.collector_outcome == "success"
    success = collector_succeeded and accepted_mode

    checked_at = None
    if isinstance(runtime, dict):
        checked_at = runtime.get("last_checked_at")
    if not checked_at and success:
        checked_at = snapshot_collected_at(snapshot)
    checked_at = str(checked_at or utc_now_iso())

    runtime_changed = runtime.get("changed") if isinstance(runtime, dict) else None
    if not isinstance(runtime_changed, bool):
        runtime_changed = None

    if success:
        content_sha = sha256_file(content_path) or previous.get("content_sha256")
        competence = infer_competence(runtime, snapshot, args.competence_mode)
        if runtime_changed is True:
            last_changed_at = (
                runtime.get("last_changed_at") if isinstance(runtime, dict) else None
            ) or checked_at
        elif runtime_changed is False:
            last_changed_at = previous.get("last_changed_at")
        else:
            last_changed_at = previous.get("last_changed_at") or snapshot_collected_at(snapshot) or checked_at

        last_successful_check_at = checked_at
        changed_on_last_success = runtime_changed
        if changed_on_last_success is None and not previous:
            changed_on_last_success = True
        last_error = None
    else:
        content_sha = previous.get("content_sha256")
        competence = previous.get("competence") or {"kind": "unknown", "value": None}
        last_changed_at = previous.get("last_changed_at")
        last_successful_check_at = previous.get("last_successful_check_at")
        changed_on_last_success = previous.get("changed_on_last_success")
        if args.error:
            last_error = args.error
        elif not collector_succeeded:
            last_error = f"collector_outcome={args.collector_outcome}"
        else:
            last_error = f"mode_not_accepted={runtime_mode!r}"

    payload: Dict[str, Any] = {
        "schema": SCHEMA,
        "source": args.source,
        "last_checked_at": checked_at,
        "last_check_status": "success" if success else "failure",
        "last_successful_check_at": last_successful_check_at,
        "last_changed_at": last_changed_at,
        "changed_on_last_success": changed_on_last_success,
        "mode_used": runtime_mode or previous.get("mode_used"),
        "content_sha256": content_sha,
        "competence": competence,
        "snapshot_collected_at": snapshot_collected_at(snapshot) or previous.get("snapshot_collected_at"),
        "last_error": last_error,
        "provenance": {
            "runtime_file": str(runtime_path) if runtime_path else None,
            "snapshot_file": str(snapshot_path) if snapshot_path else None,
            "content_file": str(content_path) if content_path else None,
        },
    }

    if isinstance(runtime, dict):
        for key in ("record_count", "records", "operational_rows", "source_sha256", "raw_hash", "stage_items_hash"):
            if key in runtime:
                payload.setdefault("runtime_summary", {})[key] = runtime[key]
    if "runtime_summary" not in payload and isinstance(previous.get("runtime_summary"), dict):
        payload["runtime_summary"] = previous["runtime_summary"]

    write_json(state_path, payload)
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
