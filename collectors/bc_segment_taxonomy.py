#!/usr/bin/env python3
"""Coleta a taxonomia oficial de segmentos de consórcio publicada pelo BCB.

Regra operacional:
- códigos novos publicados pelo BCB são adições seguras e entram automaticamente;
- códigos já conhecidos preservam key e label de exibição do frontend;
- desaparecimento de código ou mudança material de significado é registrado como
  mudança estrutural e bloqueia a publicação automática até revisão humana.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

TAXONOMY_CONTRACT = "segment-taxonomy.v1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError(f"JSON inválido em {path}")
    return payload


def dump_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2, sort_keys=False)
        fh.write("\n")


def append_github_output(name: str, value: str) -> None:
    output = os.environ.get("GITHUB_OUTPUT")
    if not output:
        return
    with open(output, "a", encoding="utf-8") as fh:
        fh.write(f"{name}={value}\n")


def normalize_key(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def normalize_semantic_label(value: Any) -> str:
    return normalize_key(value)


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


def parse_official_segments(html: bytes) -> Dict[str, str]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True).replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)

    anchor = text.lower().find("segmentos consolidados")
    if anchor < 0:
        raise ValueError("Página oficial sem âncora 'Segmentos Consolidados'")

    tail = text[anchor:]
    match_start = re.search(r"(?:\*\s*)?1\s*=\s*", tail)
    if not match_start:
        raise ValueError("Página oficial sem legenda de segmentos iniciada em '1 ='")

    legend = tail[match_start.start():]
    stop_candidates = [
        legend.lower().find("dados por unidade da federação"),
        legend.lower().find("dados por unidade da federacao"),
        legend.lower().find("dados contábeis consolidados"),
        legend.lower().find("dados contabeis consolidados"),
    ]
    stop_candidates = [x for x in stop_candidates if x > 0]
    if stop_candidates:
        legend = legend[:min(stop_candidates)]

    entries: Dict[str, str] = {}
    pattern = re.compile(
        r"(?:\*\s*)?(\d+)\s*=\s*(.*?)"
        r"(?=;\s*\d+\s*=|\s+\d+\.\s+Dados\s+por\s+Unidade|$)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for code, raw_label in pattern.findall(legend):
        label = re.sub(r"\s+", " ", raw_label).strip(" ;.*")
        if code and label:
            entries[str(int(code))] = label

    if len(entries) < 6:
        raise ValueError(
            f"Legenda oficial incompleta: esperados ao menos 6 códigos, encontrados {len(entries)}"
        )
    if "1" not in entries:
        raise ValueError("Legenda oficial não contém o segmento 1")
    return entries


def current_competence(stage_dir: Path) -> Optional[str]:
    catalog = stage_dir / "consorciobd_catalog.json"
    if not catalog.exists():
        return None
    try:
        payload = load_json(catalog)
        resource = payload.get("resource") if isinstance(payload.get("resource"), dict) else {}
        candidate = resource.get("selected_candidate") if isinstance(resource.get("selected_candidate"), dict) else {}
        value = str(candidate.get("yyyymm") or "")
        return value if re.fullmatch(r"20\d{4}", value) else None
    except Exception:
        return None


def unique_key(label: str, code: str, used_keys: set[str]) -> str:
    base = normalize_key(label) or f"segmento_{code}"
    key = base
    if key in used_keys:
        key = f"{base}_{code}"
    suffix = 2
    while key in used_keys:
        key = f"{base}_{code}_{suffix}"
        suffix += 1
    return key


def display_label_from_official(label: str) -> str:
    label = re.sub(r"\s+", " ", label).strip()
    return label[:1].upper() + label[1:] if label else label


def main() -> int:
    parser = argparse.ArgumentParser(description="Coleta taxonomia oficial de segmentos do BCB")
    parser.add_argument("--config", required=True)
    parser.add_argument("--source", default="bc_consorciobd")
    args = parser.parse_args()

    cfg = load_json(Path(args.config))
    source_cfg = cfg.get("sources", {}).get(args.source)
    if not isinstance(source_cfg, dict):
        raise ValueError(f"Fonte {args.source!r} ausente em config")

    official_url = str(source_cfg.get("official_page_url") or "").strip()
    if not official_url:
        raise ValueError("Fonte ConsorcioBD sem official_page_url")

    stage_dir = Path(source_cfg["storage"]["stage_dir"])
    stage_file = stage_dir / "segment_taxonomy.json"
    runtime_file = Path("data/runtime/bc_segment_taxonomy.json")
    if not stage_file.exists():
        raise ValueError(
            "Baseline versionado de taxonomia ausente; não é seguro recriar keys históricas sem referência"
        )

    previous = load_json(stage_file)
    if previous.get("contract") != TAXONOMY_CONTRACT:
        raise ValueError("Baseline de taxonomia com contrato incompatível")
    previous_items = previous.get("items")
    if not isinstance(previous_items, list) or not previous_items:
        raise ValueError("Baseline de taxonomia sem items")

    defaults = cfg.get("defaults", {})
    timeout_seconds = int(defaults.get("timeout_seconds", 60))
    user_agent = str(
        defaults.get(
            "user_agent",
            "comparador-consorcios-data/2.0 (+https://sanida.com.br/financas/consorcio/)",
        )
    )
    session = build_session(timeout_seconds)
    response = session.get(
        official_url,
        headers={"Accept": "text/html,*/*;q=0.8", "User-Agent": user_agent},
        timeout=getattr(session, "request_timeout", 60),
        allow_redirects=True,
    )
    response.raise_for_status()
    html = response.content
    if not html:
        raise ValueError("Página oficial de taxonomia retornou conteúdo vazio")

    official = parse_official_segments(html)
    fetched_at = utc_now_iso()
    competence = current_competence(stage_dir)

    previous_by_code = {
        str(item.get("codigo")): item
        for item in previous_items
        if isinstance(item, dict) and str(item.get("codigo") or "").isdigit()
    }
    previous_codes = set(previous_by_code)
    official_codes = set(official)

    missing_codes = sorted(previous_codes - official_codes, key=int)
    new_codes = sorted(official_codes - previous_codes, key=int)
    renamed_codes: List[Dict[str, str]] = []

    for code in sorted(previous_codes & official_codes, key=int):
        old_label = str(previous_by_code[code].get("label_oficial") or "").strip()
        new_label = official[code]
        if normalize_semantic_label(old_label) != normalize_semantic_label(new_label):
            renamed_codes.append({
                "codigo": code,
                "label_anterior": old_label,
                "label_observado": new_label,
            })

    used_keys = {
        str(item.get("key"))
        for item in previous_items
        if isinstance(item, dict) and item.get("key")
    }

    items: List[Dict[str, Any]] = []
    renamed_set = {item["codigo"] for item in renamed_codes}
    for code in sorted(previous_codes, key=int):
        item = dict(previous_by_code[code])
        if code in official and code not in renamed_set:
            item["label_oficial"] = official[code]
        items.append(item)

    for code in new_codes:
        label = official[code]
        key = unique_key(label, code, used_keys)
        used_keys.add(key)
        items.append({
            "codigo": code,
            "key": key,
            "label_oficial": label,
            "label_exibicao": display_label_from_official(label),
            "first_seen": competence,
        })

    items.sort(key=lambda item: int(str(item["codigo"])))

    structural = bool(missing_codes or renamed_codes)
    status = (
        "structural_change_detected"
        if structural
        else ("ok_with_additions" if new_codes else "ok")
    )

    payload = {
        "contract": TAXONOMY_CONTRACT,
        "source": "bc_consorciobd_segment_taxonomy",
        "source_url": response.url,
        "fetched_at": fetched_at,
        "source_sha256": hashlib.sha256(html).hexdigest(),
        "status": status,
        "auto_added_codes": new_codes,
        "missing_codes": missing_codes,
        "renamed_codes": renamed_codes,
        "items": items,
    }

    previous_text = json.dumps(previous, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
    current_text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
    changed = previous_text != current_text
    if changed:
        dump_json(stage_file, payload)

    runtime_payload = {
        "source": "bc_consorciobd_segment_taxonomy",
        "last_checked_at": fetched_at,
        "changed": changed,
        "status": status,
        "official_codes": sorted(official_codes, key=int),
        "auto_added_codes": new_codes,
        "missing_codes": missing_codes,
        "renamed_codes": renamed_codes,
        "stage_file": str(stage_file),
        "source_url": response.url,
    }
    dump_json(runtime_file, runtime_payload)

    append_github_output("changed", "true" if changed else "false")
    append_github_output("status", status)
    append_github_output("auto_added_codes", ",".join(new_codes))
    append_github_output("segment_count", str(len(items)))
    append_github_output("stage_file", str(stage_file))

    print(json.dumps(runtime_payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Falha no coletor de taxonomia oficial: {exc}", file=sys.stderr)
        raise SystemExit(1)
