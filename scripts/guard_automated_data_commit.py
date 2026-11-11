#!/usr/bin/env python3
"""Restrict staged changes from V2 automatic writers to explicit data files.

This is defense in depth for a narrowly scoped GitHub App that bypasses
the main branch PR rule. Server-side rulesets and PR CI remain mandatory.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Iterable

_SOURCE_STATES = frozenset(
    f"data/source_state/{name}.json"
    for name in (
        "bc_cadastro_admins",
        "bc_filiais",
        "bc_consorciobd",
        "bc_ranking_reclamacoes",
    )
)
_RELEASE_FILES = frozenset(
    f"data/dist-v2/global/{name}.json"
    for name in (
        "instituicoes",
        "administradoras",
        "produtos",
        "rankings",
        "segmentos",
        "comparacoes",
        "ofertas",
        "meta",
    )
)
ALLOWED_FILES: dict[str, frozenset[str]] = {
    "cadastro": frozenset(
        {
            "data/source_state/bc_cadastro_admins.json",
            "data/raw/bc/cadastro/latest.json",
            "data/stage/cadastro/instituicoes_cadastro.json",
        }
    ),
    "filiais": frozenset(
        {
            "data/source_state/bc_filiais.json",
            "data/raw/bc/filiais/latest.json",
            "data/stage/filiais/filiais.json",
        }
    ),
    "consorciobd": frozenset(
        {
            "data/source_state/bc_consorciobd.json",
            "data/raw/bc/consorciobd/latest.json",
            "data/raw/bc/consorciobd/latest_source.bin",
            "data/stage/consorciobd/consorciobd_catalog.json",
            "data/stage/consorciobd/segment_taxonomy.json",
        }
    ),
    "reclamacoes": frozenset(
        {
            "data/source_state/bc_ranking_reclamacoes.json",
            "data/raw/bc/ranking_reclamacoes/latest.json",
            "data/raw/bc/ranking_reclamacoes/latest_source.csv",
            "data/stage/ranking_reclamacoes/ranking_reclamacoes_consorcios.json",
        }
    ),
    "release": _RELEASE_FILES | _SOURCE_STATES,
}


def unexpected_paths(workflow: str, paths: Iterable[str]) -> list[str]:
    """Return every staged path that the chosen workflow is forbidden to write."""
    allowed = ALLOWED_FILES[workflow]
    return sorted({path for path in paths if path not in allowed})


def staged_paths() -> list[str]:
    # --no-renames ensures source and destination of renames are both checked.
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--no-renames", "-z"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    return [
        path.decode("utf-8", errors="surrogateescape")
        for path in result.stdout.split(b"\0")
        if path
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow", required=True, choices=sorted(ALLOWED_FILES))
    args = parser.parse_args()
    try:
        paths = staged_paths()
    except subprocess.CalledProcessError as exc:
        print(f"Failed to inspect git index: {exc.stderr.decode(errors='replace')}", file=sys.stderr)
        return 2

    forbidden = unexpected_paths(args.workflow, paths)
    if forbidden:
        print(
            f"ABORT: automated workflow {args.workflow} attempted to commit forbidden paths:",
            file=sys.stderr,
        )
        for path in forbidden:
            print(f"  {path}", file=sys.stderr)
        return 1

    print(f"Approved {len(paths)} staged data paths for workflow {args.workflow}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
