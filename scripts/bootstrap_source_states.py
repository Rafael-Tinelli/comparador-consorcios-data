#!/usr/bin/env python3
"""Bootstrap only-missing durable source states from the currently tracked snapshots.

This is a migration aid for C09. Once a state exists, collectors own subsequent
updates; this script never overwrites an existing state.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Cria estados de fonte V2 ausentes sem sobrescrever estado existente")
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    config_path = Path(args.config)
    cfg = load_json(config_path)
    sources = cfg.get("required_sources")
    if not isinstance(sources, dict) or not sources:
        raise SystemExit("config de proveniência sem required_sources")

    persist_script = Path(__file__).with_name("persist_source_state.py")
    created = []
    skipped = []

    for source, spec in sources.items():
        if not isinstance(spec, dict):
            raise SystemExit(f"spec inválida para {source}")
        state_path = Path(spec["state_file"])
        if state_path.exists():
            skipped.append(source)
            continue

        snapshot = Path(spec["snapshot_file"])
        content = Path(spec["content_file"])
        if not snapshot.exists() or not content.exists():
            raise SystemExit(f"não é possível bootstrap {source}: snapshot/content ausente")

        cmd = [
            sys.executable,
            str(persist_script),
            "--source", source,
            "--snapshot", str(snapshot),
            "--content-file", str(content),
            "--state", str(state_path),
            "--collector-outcome", "success",
            "--competence-mode", str(spec.get("competence_mode") or "auto"),
        ]
        for mode in spec.get("accepted_modes", []):
            cmd.extend(["--accepted-mode", str(mode)])
        subprocess.run(cmd, check=True)
        created.append(source)

    print(json.dumps({"created": created, "skipped": skipped}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
