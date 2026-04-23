#!/usr/bin/env python3
"""
Coletor do ranking de reclamações - Consórcios.

Estratégia:
- consulta o endpoint oficial que lista todos os rankings;
- encontra o registro mais recente do tipo Consorcios;
- usa a URL direta do próprio listing quando existir;
- faz fallback para a URL padrão documentada;
- baixa o CSV;
- grava raw, stage e runtime;
- só considera sucesso quando mode_used = "listing-api-download".
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
from datetime import datetime, timezone
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


def sha256_of_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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


def normalize_spaces(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    return text or None


def normalize_match_text(value: Optional[str]) -> str:
    text = normalize_spaces(value) or ""
    return text.lower()


def build_session(timeout_seconds: int) -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.request_timeout = timeout_seconds  # type: ignore[attr-defined]
    return session


def detect_csv_delimiter(text: str) -> str:
    sample = text[:10000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
        return dialect.delimiter
    except Exception:
        return ";" if sample.count(";") > sample.count(",") else ","


def preview_text(text: str, limit: int = 350) -> str:
    compact = " ".join((text or "").split())
    return compact[:limit]


def json_request(session: requests.Session, url: str, headers: Dict[str, str]) -> Any:
    response = session.get(
        url,
        headers=headers,
        timeout=getattr(session, "request_timeout", 60),
        allow_redirects=True,
    )
    response.raise_for_status()

    if not response.text.strip():
        raise ValueError(f"Endpoint JSON retornou corpo vazio: {url}")

    return response.json()


def extract_candidate_records(payload: Any) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            keys_lower = {str(k).lower() for k in node.keys()}
            if any(k in keys_lower for k in ("ano", "anocalendario")) and any(
                k in keys_lower for k in ("periodo", "periodicidade", "numeroperiodicidade", "numero_periodicidade")
            ):
                records.append(node)

            for value in node.values():
                walk(value)

        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return records


def pick_first_value(record: Dict[str, Any], names: Iterable[str]) -> Optional[Any]:
    lowered = {str(k).lower(): k for k in record.keys()}
    for name in names:
        key = lowered.get(name.lower())
        if key is not None:
            return record.get(key)
    return None


def parse_int_like(value: Any) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    match = re.search(r"\d+", text)
    if not match:
        return None
    try:
        return int(match.group(0))
    except Exception:
        return None


def find_url_in_record(record: Dict[str, Any]) -> Optional[str]:
    for key, value in record.items():
        if value is None:
            continue
        if isinstance(value, str) and value.startswith("http"):
            return value

    for key, value in record.items():
        if isinstance(value, dict):
            url = find_url_in_record(value)
            if url:
                return url

    return None


def record_is_target(record: Dict[str, Any], tipo_alvo: str) -> bool:
    normalized_target = normalize_match_text(tipo_alvo)
    serialized = stable_json_dumps(record).lower()
    return normalized_target in serialized


def periodicity_rank(value: Optional[str]) -> int:
    text = normalize_match_text(value)
    if "mensal" in text:
        return 3
    if "trimestral" in text:
        return 2
    if "semestral" in text:
        return 1
    return 0


def build_candidate_from_record(
    record: Dict[str, Any],
    tipo_alvo: str,
    download_template: str,
) -> Optional[Dict[str, Any]]:
    if not record_is_target(record, tipo_alvo):
        return None

    ano = parse_int_like(pick_first_value(record, ["ano", "anoCalendario"]))
    periodo = parse_int_like(pick_first_value(record, ["periodo", "numero_periodicidade", "numeroPeriodicidade"]))
    periodicidade_raw = pick_first_value(record, ["periodicidade", "tipoPeriodicidade", "descricaoPeriodicidade"])
    periodicidade = normalize_spaces(periodicidade_raw)

    if ano is None or periodo is None:
        return None

    direct_url = find_url_in_record(record)
    fallback_url = download_template.format(
        ano=ano,
        periodicidade=(periodicidade or "SEMESTRAL"),
        periodo=periodo,
    )

    return {
        "ano": ano,
        "periodo": periodo,
        "periodicidade": periodicidade or "SEMESTRAL",
        "direct_url": direct_url,
        "download_url": direct_url or fallback_url,
        "record": record,
    }


def choose_latest_candidate(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not candidates:
        raise ValueError("Nenhum ranking de Consorcios foi identificado no listing oficial.")

    candidates.sort(
        key=lambda item: (
            item["ano"],
            periodicity_rank(item.get("periodicidade")),
            item["periodo"],
        ),
        reverse=True,
    )
    return candidates[0]


def download_csv(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
) -> Tuple[bytes, Dict[str, Any], Dict[str, Any]]:
    response = session.get(
        url,
        headers=headers,
        timeout=getattr(session, "request_timeout", 120),
        allow_redirects=True,
    )
    response.raise_for_status()

    binary = response.content
    if not binary:
        raise ValueError("O download do ranking retornou conteúdo vazio.")

    text = None
    chosen_encoding = None
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            text = binary.decode(encoding)
            chosen_encoding = encoding
            break
        except Exception:
            continue

    if text is None:
        raise ValueError("Não foi possível decodificar o CSV do ranking.")

    delimiter = detect_csv_delimiter(text)
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    header = next(reader, [])
    sample_rows = []
    for _ in range(3):
        try:
            sample_rows.append(next(reader))
        except StopIteration:
            break

    download_meta = {
        "status_code": response.status_code,
        "final_url": response.url,
        "content_type": response.headers.get("Content-Type"),
        "content_length": response.headers.get("Content-Length"),
        "last_modified": response.headers.get("Last-Modified"),
        "etag": response.headers.get("ETag"),
    }

    csv_meta = {
        "encoding": chosen_encoding,
        "delimiter": delimiter,
        "header": header,
        "sample_rows": sample_rows,
        "preview": preview_text(text),
    }

    return binary, download_meta, csv_meta


def read_existing_source_sha(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    return sha256_of_bytes(path.read_bytes())


def main() -> int:
    parser = argparse.ArgumentParser(description="Coletor do ranking de reclamações - Consórcios.")
    parser.add_argument("--config", required=True, help="Caminho para config/sources.json")
    parser.add_argument("--source", default="bc_ranking_reclamacoes", help="Nome da fonte em config/sources.json")
    args = parser.parse_args()

    config = load_json(Path(args.config))
    source_cfg = get_source_config(config, args.source)
    defaults = config.get("defaults", {})

    raw_dir = Path(source_cfg["storage"]["raw_dir"])
    stage_dir = Path(source_cfg["storage"]["stage_dir"])
    runtime_dir = Path("data/runtime")

    raw_file = raw_dir / "latest.json"
    raw_source_file = raw_dir / "latest_source.csv"
    stage_file = stage_dir / "ranking_reclamacoes_consorcios.json"
    runtime_file = runtime_dir / "bc_ranking_reclamacoes.json"

    raw_dir.mkdir(parents=True, exist_ok=True)
    stage_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    collected_at = utc_now_iso()
    mode_used = "listing-api-failed"

    if not source_cfg.get("enabled", False):
        runtime_payload = {
            "source": args.source,
            "last_checked_at": collected_at,
            "changed": False,
            "mode_used": "disabled",
            "raw_file": str(raw_file),
            "raw_source_file": str(raw_source_file),
            "stage_file": str(stage_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("mode_used", "disabled")
        print(f"Fonte '{args.source}' está desabilitada.")
        return 0

    api_cfg = source_cfg["api"]
    tipo_alvo = api_cfg.get("tipo_alvo", "Consorcios")

    user_agent = defaults.get(
        "user_agent",
        "comparador-consorcios-data/1.0 (+https://sanida.com.br/financas/consorcio/)",
    )
    timeout_seconds = int(defaults.get("timeout_seconds", 60))
    session = build_session(timeout_seconds=timeout_seconds)

    json_headers = {
        "Accept": "application/json",
        "User-Agent": user_agent,
    }
    download_headers = {
        "Accept": "*/*",
        "User-Agent": user_agent,
        "Referer": source_cfg["official_page_url"],
    }

    try:
        listing_payload = json_request(session, api_cfg["list_endpoint"], json_headers)
        extracted_records = extract_candidate_records(listing_payload)

        candidates: List[Dict[str, Any]] = []
        for record in extracted_records:
            candidate = build_candidate_from_record(
                record=record,
                tipo_alvo=tipo_alvo,
                download_template=api_cfg["download_endpoint_template"],
            )
            if candidate:
                candidates.append(candidate)

        selected = choose_latest_candidate(candidates)
        binary, download_meta, csv_meta = download_csv(
            session=session,
            url=selected["download_url"],
            headers=download_headers,
        )

        previous_sha = read_existing_source_sha(raw_source_file)
        current_sha = sha256_of_bytes(binary)
        changed = previous_sha != current_sha
        mode_used = "listing-api-download"

        raw_payload = {
            "metadata": {
                "source": args.source,
                "collector": "collectors/bc_ranking_reclamacoes.py",
                "collected_at": collected_at,
                "mode_used": mode_used,
                "listing_endpoint": api_cfg["list_endpoint"],
                "resource_sha256": current_sha,
                "candidate_count": len(candidates),
                "extracted_record_count": len(extracted_records),
            },
            "selected": {
                "ano": selected["ano"],
                "periodo": selected["periodo"],
                "periodicidade": selected["periodicidade"],
                "download_url": selected["download_url"],
                "direct_url_from_listing": selected["direct_url"],
                "download_meta": download_meta,
                "csv_meta": csv_meta,
                "source_record": selected["record"],
            },
            "top_candidates": candidates[:20],
        }

        stage_payload = {
            "metadata": {
                "source": args.source,
                "collector": "collectors/bc_ranking_reclamacoes.py",
                "collected_at": collected_at,
                "mode_used": mode_used,
                "resource_sha256": current_sha,
            },
            "selected": {
                "ano": selected["ano"],
                "periodo": selected["periodo"],
                "periodicidade": selected["periodicidade"],
                "download_url": selected["download_url"],
                "download_meta": download_meta,
                "csv_meta": csv_meta,
            },
            "top_candidates": candidates[:20],
        }

        runtime_payload = {
            "source": args.source,
            "last_checked_at": collected_at,
            "last_changed_at": collected_at if changed else None,
            "changed": changed,
            "mode_used": mode_used,
            "candidate_count": len(candidates),
            "selected_ano": selected["ano"],
            "selected_periodo": selected["periodo"],
            "selected_periodicidade": selected["periodicidade"],
            "selected_url": selected["download_url"],
            "resource_sha256": current_sha,
            "raw_file": str(raw_file),
            "raw_source_file": str(raw_source_file),
            "stage_file": str(stage_file),
        }

        if changed:
            raw_source_file.write_bytes(binary)
            dump_json(raw_file, raw_payload)
            dump_json(stage_file, stage_payload)

        dump_json(runtime_file, runtime_payload)

        append_github_output("changed", "true" if changed else "false")
        append_github_output("records", str(len(candidates)))
        append_github_output("stage_hash", sha256_of(stage_payload))
        append_github_output("mode_used", mode_used)

        print(
            json.dumps(
                {
                    "source": args.source,
                    "changed": changed,
                    "records": len(candidates),
                    "mode_used": mode_used,
                    "raw_file": str(raw_file),
                    "raw_source_file": str(raw_source_file),
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
            "mode_used": mode_used,
            "error": f"Erro HTTP ao coletar ranking de reclamações: {exc}",
            "raw_file": str(raw_file),
            "raw_source_file": str(raw_source_file),
            "stage_file": str(stage_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("mode_used", mode_used)
        print(f"Erro HTTP ao coletar ranking de reclamações: {exc}", file=sys.stderr)
        return 1

    except Exception as exc:
        runtime_payload = {
            "source": args.source,
            "last_checked_at": collected_at,
            "changed": False,
            "mode_used": mode_used,
            "error": str(exc),
            "raw_file": str(raw_file),
            "raw_source_file": str(raw_source_file),
            "stage_file": str(stage_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("mode_used", mode_used)
        print(f"Falha no coletor bc_ranking_reclamacoes: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
