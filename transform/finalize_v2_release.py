#!/usr/bin/env python3
"""Attach durable source provenance to a generated Comparador V2 release.

The builder's generated_at describes read-model generation, not source freshness.
This finalizer injects the durable per-source check/change/competence state into
`global/meta.json`, which is the publication contract consumed by HostGator/UI.
It also proves that the durable state's content hash matches the exact file used
by the build, preventing provenance from drifting away from the consumed bytes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict

RELEASE_CONTRACT = "comparador-v2-release.v1"
SOURCE_STATE_SCHEMA = "source-state.v1"


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical_sha(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def write_if_changed(path: Path, payload: Dict[str, Any]) -> bool:
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
    old = path.read_text(encoding="utf-8") if path.exists() else None
    if old == text:
        return False
    path.write_text(text, encoding="utf-8")
    return True


def compact_state(state: Dict[str, Any], role: str) -> Dict[str, Any]:
    return {
        "role": role,
        "last_checked_at": state.get("last_checked_at"),
        "last_check_status": state.get("last_check_status"),
        "last_successful_check_at": state.get("last_successful_check_at"),
        "last_changed_at": state.get("last_changed_at"),
        "changed_on_last_success": state.get("changed_on_last_success"),
        "mode_used": state.get("mode_used"),
        "content_sha256": state.get("content_sha256"),
        "competence": state.get("competence"),
        "snapshot_collected_at": state.get("snapshot_collected_at"),
        "last_error": state.get("last_error"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Finaliza meta V2 com proveniência persistente das fontes")
    parser.add_argument("--dist-base", required=True)
    parser.add_argument("--provenance-config", required=True)
    args = parser.parse_args()

    dist_base = Path(args.dist_base)
    meta_path = dist_base / "global" / "meta.json"
    if not meta_path.exists():
        raise SystemExit(f"meta V2 ausente: {meta_path}")

    meta = load_json(meta_path)
    if not isinstance(meta, dict):
        raise SystemExit("meta V2 inválido")

    config = load_json(Path(args.provenance_config))
    required_sources = config.get("required_sources")
    if not isinstance(required_sources, dict) or not required_sources:
        raise SystemExit("provenance config sem required_sources")

    source_status: Dict[str, Any] = {}
    blocking_errors = []
    degraded_sources = []

    for source, spec in required_sources.items():
        if not isinstance(spec, dict):
            blocking_errors.append(f"spec inválida para {source}")
            continue

        state_path = Path(str(spec.get("state_file") or ""))
        content_path = Path(str(spec.get("content_file") or ""))
        if not state_path.exists():
            blocking_errors.append(f"estado persistente ausente: {source} -> {state_path}")
            continue
        if not content_path.exists() or not content_path.is_file():
            blocking_errors.append(f"conteúdo canônico ausente: {source} -> {content_path}")
            continue

        state = load_json(state_path)
        if not isinstance(state, dict) or state.get("schema") != SOURCE_STATE_SCHEMA:
            blocking_errors.append(f"schema de estado inválido: {source}")
            continue
        if state.get("source") != source:
            blocking_errors.append(f"source divergente no estado: {source}")
            continue
        if state.get("last_check_status") not in {"success", "failure"}:
            blocking_errors.append(f"last_check_status inválido: {source}")
        if not state.get("last_checked_at"):
            blocking_errors.append(f"last_checked_at ausente: {source}")
        if not state.get("last_successful_check_at"):
            blocking_errors.append(f"last_successful_check_at ausente: {source}")

        state_hash = state.get("content_sha256")
        if not isinstance(state_hash, str) or len(state_hash) != 64:
            blocking_errors.append(f"content_sha256 ausente/inválido: {source}")
        else:
            actual_hash = sha256_file(content_path)
            if actual_hash != state_hash:
                blocking_errors.append(
                    f"content_sha256 não corresponde aos bytes consumidos: {source} estado={state_hash} arquivo={actual_hash}"
                )

        provenance = state.get("provenance")
        if isinstance(provenance, dict):
            state_content_file = provenance.get("content_file")
            if state_content_file and str(state_content_file) != str(content_path):
                blocking_errors.append(
                    f"content_file do estado diverge da configuração: {source} estado={state_content_file} config={content_path}"
                )

        competence = state.get("competence")
        if not isinstance(competence, dict) or "kind" not in competence or "value" not in competence:
            blocking_errors.append(f"competence inválida: {source}")
        else:
            competence_mode = str(spec.get("competence_mode") or "auto")
            if competence_mode == "not_applicable":
                if competence.get("kind") != "not_applicable":
                    blocking_errors.append(f"competence deveria ser not_applicable: {source}")
            else:
                if competence.get("kind") in {"unknown", "not_reported", "not_applicable"} or competence.get("value") in {None, ""}:
                    blocking_errors.append(f"competence não resolvida para fonte que exige período/data: {source}")

        compact = compact_state(state, str(spec.get("role") or "source"))
        source_status[source] = compact
        if compact.get("last_check_status") != "success":
            degraded_sources.append(source)

    monthly_period = meta.get("source_periods", {}).get("consorciobd_mensal")
    monthly_state = source_status.get("bc_consorciobd")
    if isinstance(monthly_state, dict):
        state_period = monthly_state.get("competence", {}).get("value") if isinstance(monthly_state.get("competence"), dict) else None
        if monthly_period and state_period != monthly_period:
            blocking_errors.append(
                f"competência ConsorcioBD diverge entre build e source_state: meta={monthly_period} estado={state_period}"
            )

    if blocking_errors:
        raise SystemExit("Proveniência V2 inválida:\n- " + "\n- ".join(blocking_errors))

    state_fingerprint = canonical_sha(source_status)
    release_fingerprint = canonical_sha({
        "pipeline_version": meta.get("pipeline_version"),
        "source_fingerprint": meta.get("source_fingerprint"),
        "methodology_sha256": meta.get("methodology_sha256"),
        "source_state_sha256": state_fingerprint,
        "artifacts": meta.get("artifacts"),
    })

    meta["source_status"] = source_status
    meta["freshness"] = {
        "semantics": {
            "generated_at": "momento de geração dos read models; não significa atualização das fontes",
            "last_checked_at": "última tentativa registrada de consulta à fonte",
            "last_successful_check_at": "última consulta bem-sucedida",
            "last_changed_at": "última consulta em que o conteúdo persistido mudou",
            "competence": "período/data a que o conteúdo da fonte se refere, quando aplicável",
        },
        "all_required_states_present": True,
        "source_state_matches_consumed_bytes": True,
        "degraded_sources": sorted(degraded_sources),
        "source_state_sha256": state_fingerprint,
    }
    meta["backend_release"] = {
        "contract": RELEASE_CONTRACT,
        "publication_eligible": True,
        "publication_note": (
            "Uma falha de consulta não apaga o último conteúdo aprovado; ela é exposta em source_status. "
            "A publicação é bloqueada se faltar estado persistente, último sucesso, competência explícita "
            "ou se o hash persistido não corresponder exatamente aos bytes consumidos pelo build."
        ),
        "release_fingerprint": release_fingerprint,
    }

    changed = write_if_changed(meta_path, meta)
    print(json.dumps({
        "changed": changed,
        "publication_eligible": True,
        "degraded_sources": sorted(degraded_sources),
        "release_fingerprint": release_fingerprint,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
