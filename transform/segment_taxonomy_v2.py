#!/usr/bin/env python3
"""Taxonomia oficial e evergreen dos segmentos do Comparador de Consórcios V2.

A taxonomia é coletada da página oficial do Banco Central e persistida em stage.
Novos códigos oficiais entram automaticamente. Renomeações ou desaparecimentos de
códigos já conhecidos não são aplicados silenciosamente: ficam marcados como
mudança estrutural para revisão humana.
"""
from __future__ import annotations

import csv
import io
import json
import re
import unicodedata
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple

TAXONOMY_CONTRACT = "segment-taxonomy.v1"
SAFE_STATUSES = {"ok", "ok_with_additions"}


def normalize_key(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def load_taxonomy(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Taxonomia de segmentos deve ser objeto JSON")
    if payload.get("contract") != TAXONOMY_CONTRACT:
        raise ValueError(
            f"Contrato de taxonomia inválido: {payload.get('contract')!r} != {TAXONOMY_CONTRACT!r}"
        )

    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError("Taxonomia de segmentos sem items")

    codes: Set[str] = set()
    keys: Set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("Item de taxonomia inválido")
        code = str(item.get("codigo") or "")
        key = str(item.get("key") or "")
        label = str(item.get("label_oficial") or "").strip()
        if not code.isdigit():
            raise ValueError(f"Código de segmento inválido: {code!r}")
        if not key or normalize_key(key) != key:
            raise ValueError(f"Key de segmento inválida: {key!r}")
        if not label:
            raise ValueError(f"Label oficial ausente no segmento {code}")
        if code in codes:
            raise ValueError(f"Código de segmento duplicado: {code}")
        if key in keys:
            raise ValueError(f"Key de segmento duplicada: {key}")
        codes.add(code)
        keys.add(key)

    return payload


def segment_map(payload: Dict[str, Any]) -> Dict[str, Tuple[str, str]]:
    return {
        str(item["codigo"]): (str(item["key"]), str(item["label_oficial"]).strip())
        for item in payload["items"]
    }


def taxonomy_codes(payload: Dict[str, Any]) -> Set[str]:
    return {str(item["codigo"]) for item in payload["items"]}


def _detect_csv_delimiter(text: str) -> str:
    try:
        return csv.Sniffer().sniff(text[:10000], delimiters=",;|\t").delimiter
    except Exception:
        return ";" if text[:10000].count(";") > text[:10000].count(",") else ","


def _parse_csv_bytes(binary: bytes) -> List[Dict[str, Any]]:
    text = None
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            text = binary.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise ValueError("Não foi possível decodificar Segmentos_Consolidados.csv")
    delimiter = _detect_csv_delimiter(text)
    return [
        {normalize_key(k): v for k, v in row.items()}
        for row in csv.DictReader(io.StringIO(text), delimiter=delimiter)
    ]


def observed_segment_codes(consorciobd_zip: Path) -> Set[str]:
    codes: Set[str] = set()
    found_consolidated = False
    with zipfile.ZipFile(consorciobd_zip, "r") as zf:
        for member in zf.namelist():
            member_key = normalize_key(member)
            if not member.lower().endswith(".csv"):
                continue
            if "segmentos_consolidados" not in member_key:
                continue
            found_consolidated = True
            for row in _parse_csv_bytes(zf.read(member)):
                raw = (
                    row.get("codigo_do_segmento")
                    or row.get("codigo_segmento")
                    or row.get("segmento")
                    or ""
                )
                code = re.sub(r"\D+", "", str(raw))
                if code:
                    codes.add(code)
    if not found_consolidated:
        raise ValueError("ConsorcioBD sem Segmentos_Consolidados.csv")
    if not codes:
        raise ValueError("ConsorcioBD sem códigos de segmento observáveis")
    return codes


def assert_release_safe(payload: Dict[str, Any], observed_codes: Iterable[str]) -> None:
    status = str(payload.get("status") or "")
    if status not in SAFE_STATUSES:
        raise ValueError(
            "Taxonomia oficial contém mudança estrutural pendente; publicação automática interrompida"
        )

    known = taxonomy_codes(payload)
    observed = {str(code) for code in observed_codes}
    unknown = sorted(observed - known, key=lambda x: int(x) if x.isdigit() else x)
    if unknown:
        raise ValueError(
            "ConsorcioBD contém códigos oficiais ainda ausentes da taxonomia coletada: "
            + ", ".join(unknown)
        )


def taxonomy_summary(payload: Dict[str, Any], observed_codes: Iterable[str]) -> Dict[str, Any]:
    known = sorted(taxonomy_codes(payload), key=int)
    observed = sorted({str(code) for code in observed_codes}, key=int)
    return {
        "contract": TAXONOMY_CONTRACT,
        "source": payload.get("source"),
        "source_url": payload.get("source_url"),
        "status": payload.get("status"),
        "known_codes": known,
        "observed_codes": observed,
        "auto_added_codes": payload.get("auto_added_codes", []),
        "fetched_at": payload.get("fetched_at"),
    }
