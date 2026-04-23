#!/usr/bin/env python3
"""
Coletor do ranking de reclamações - Consórcios.

Estratégia desta versão:
- abandona dependência crítica do endpoint de listagem;
- usa o padrão oficial documentado do recurso de Consórcios:
  .../ranking/arquivo?ano={ano}&periodicidade=SEMESTRAL&periodo={periodo}&tipo=Consorcios
- faz probing retroativo por semestres;
- baixa o CSV mais recente disponível;
- grava raw, stage e runtime;
- só considera sucesso quando mode_used = "direct-content-download".
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
from typing import Any, Dict, List, Optional, Tuple

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


def candidate_looks_valid_from_headers(
    *,
    status_code: Optional[int],
    content_type: Optional[str],
    content_length: Optional[str],
    min_size_bytes: int,
) -> bool:
    if status_code != 200:
        return False

    ct = (content_type or "").lower()
    if "text/html" in ct:
        return False

    if content_length:
        try:
            if int(content_length) < min_size_bytes:
                return False
        except Exception:
            pass

    return True


def probe_head(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    min_size_bytes: int,
) -> Dict[str, Any]:
    try:
        response = session.head(
            url,
            headers=headers,
            timeout=getattr(session, "request_timeout", 60),
            allow_redirects=True,
        )
        result = {
            "method": "HEAD",
            "status_code": response.status_code,
            "final_url": response.url,
            "content_type": response.headers.get("Content-Type"),
            "content_length": response.headers.get("Content-Length"),
        }
        result["ok"] = candidate_looks_valid_from_headers(
            status_code=result["status_code"],
            content_type=result["content_type"],
            content_length=result["content_length"],
            min_size_bytes=min_size_bytes,
        )
        return result
    except Exception as exc:
        return {
            "method": "HEAD",
            "status_code": None,
            "final_url": url,
            "content_type": None,
            "content_length": None,
            "ok": False,
            "error": str(exc),
        }


def probe_get(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    min_size_bytes: int,
) -> Dict[str, Any]:
    try:
        response = session.get(
            url,
            headers=headers,
            timeout=getattr(session, "request_timeout", 90),
            allow_redirects=True,
            stream=True,
        )

        first_chunk = b""
        try:
            for chunk in response.iter_content(chunk_size=4096):
                if chunk:
                    first_chunk = chunk
                    break
        finally:
            response.close()

        result = {
            "method": "GET",
            "status_code": response.status_code,
            "final_url": response.url,
            "content_type": response.headers.get("Content-Type"),
            "content_length": response.headers.get("Content-Length"),
            "first_chunk_sha256": sha256_of_bytes(first_chunk) if first_chunk else None,
            "first_chunk_preview_hex": first_chunk[:32].hex() if first_chunk else None,
        }
        result["ok"] = candidate_looks_valid_from_headers(
            status_code=result["status_code"],
            content_type=result["content_type"],
            content_length=result["content_length"],
            min_size_bytes=min_size_bytes,
        )
        return result
    except Exception as exc:
        return {
            "method": "GET",
            "status_code": None,
            "final_url": url,
            "content_type": None,
            "content_length": None,
            "ok": False,
            "error": str(exc),
        }


def iter_semesters(reference_year: int, semesters_back: int) -> List[Tuple[int, int]]:
    items: List[Tuple[int, int]] = []
    year = reference_year
    period = 2

    for _ in range(semesters_back):
        items.append((year, period))
        period -= 1
        if period == 0:
            period = 2
            year -= 1

    return items


def render_url(download_template: str, year: int, period: int, periodicidade: str, tipo: str) -> str:
    return download_template.format(
        ano=year,
        periodicidade=periodicidade,
        periodo=period,
        tipo=tipo,
    )


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
    mode_used = "direct-probe-failed"

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
    periodicidade = api_cfg.get("periodicidade", "SEMESTRAL")
    tipo = api_cfg.get("tipo", "Consorcios")
    semesters_back = int(api_cfg.get("probe_semesters_back", 10))
    min_size_bytes = int(api_cfg.get("min_size_bytes", 200))
    http_methods = [m.upper() for m in api_cfg.get("http_methods", ["HEAD", "GET"])]

    user_agent = defaults.get(
        "user_agent",
        "comparador-consorcios-data/1.0 (+https://sanida.com.br/financas/consorcio/)",
    )
    timeout_seconds = int(defaults.get("timeout_seconds", 60))
    session = build_session(timeout_seconds=timeout_seconds)

    headers = {
        "Accept": "*/*",
        "User-Agent": user_agent,
        "Referer": source_cfg["official_page_url"],
    }

    current_year = datetime.now(timezone.utc).year
    probe_results: List[Dict[str, Any]] = []
    selected_candidate: Optional[Dict[str, Any]] = None
    selected_probe: Optional[Dict[str, Any]] = None

    for year, period in iter_semesters(current_year, semesters_back):
        url = render_url(
            download_template=api_cfg["download_endpoint_template"],
            year=year,
            period=period,
            periodicidade=periodicidade,
            tipo=tipo,
        )
        candidate = {
            "ano": year,
            "periodo": period,
            "periodicidade": periodicidade,
            "tipo": tipo,
            "url": url,
        }

        probes: List[Dict[str, Any]] = []
        head_result: Optional[Dict[str, Any]] = None
        get_result: Optional[Dict[str, Any]] = None

        if "HEAD" in http_methods:
            head_result = probe_head(session, url, headers, min_size_bytes)
            probes.append(head_result)

        if head_result and head_result.get("ok"):
            selected_candidate = candidate
            selected_probe = head_result
            probe_results.append({"candidate": candidate, "probes": probes})
            break

        if "GET" in http_methods:
            get_result = probe_get(session, url, headers, min_size_bytes)
            probes.append(get_result)

        probe_results.append({"candidate": candidate, "probes": probes})

        if get_result and get_result.get("ok"):
            selected_candidate = candidate
            selected_probe = get_result
            break

    if not selected_candidate:
        runtime_payload = {
            "source": args.source,
            "last_checked_at": collected_at,
            "changed": False,
            "mode_used": mode_used,
            "probe_results": probe_results,
            "raw_file": str(raw_file),
            "raw_source_file": str(raw_source_file),
            "stage_file": str(stage_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", str(len(probe_results)))
        append_github_output("mode_used", mode_used)
        raise ValueError("Nenhum ranking de Consórcios foi localizado pelo padrão direto documentado.")

    binary, download_meta, csv_meta = download_csv(
        session=session,
        url=selected_candidate["url"],
        headers=headers,
    )

    previous_sha = read_existing_source_sha(raw_source_file)
    current_sha = sha256_of_bytes(binary)
    changed = previous_sha != current_sha
    mode_used = "direct-content-download"

    raw_payload = {
        "metadata": {
            "source": args.source,
            "collector": "collectors/bc_ranking_reclamacoes.py",
            "collected_at": collected_at,
            "mode_used": mode_used,
            "resource_sha256": current_sha,
            "probe_count": len(probe_results),
        },
        "selected": {
            "ano": selected_candidate["ano"],
            "periodo": selected_candidate["periodo"],
            "periodicidade": selected_candidate["periodicidade"],
            "tipo": selected_candidate["tipo"],
            "download_url": selected_candidate["url"],
            "selected_probe": selected_probe,
            "download_meta": download_meta,
            "csv_meta": csv_meta,
        },
        "probe_results": probe_results,
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
            "ano": selected_candidate["ano"],
            "periodo": selected_candidate["periodo"],
            "periodicidade": selected_candidate["periodicidade"],
            "tipo": selected_candidate["tipo"],
            "download_url": selected_candidate["url"],
            "download_meta": download_meta,
            "csv_meta": csv_meta,
        },
        "probe_results": probe_results,
    }

    runtime_payload = {
        "source": args.source,
        "last_checked_at": collected_at,
        "last_changed_at": collected_at if changed else None,
        "changed": changed,
        "mode_used": mode_used,
        "selected_ano": selected_candidate["ano"],
        "selected_periodo": selected_candidate["periodo"],
        "selected_periodicidade": selected_candidate["periodicidade"],
        "selected_tipo": selected_candidate["tipo"],
        "selected_url": selected_candidate["url"],
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
    append_github_output("records", str(len(probe_results)))
    append_github_output("stage_hash", sha256_of(stage_payload))
    append_github_output("mode_used", mode_used)

    print(
        json.dumps(
            {
                "source": args.source,
                "changed": changed,
                "records": len(probe_results),
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


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except requests.HTTPError as exc:
        print(f"Erro HTTP ao coletar ranking de reclamações: {exc}", file=sys.stderr)
        raise SystemExit(1)
    except Exception as exc:
        print(f"Falha no coletor bc_ranking_reclamacoes: {exc}", file=sys.stderr)
        raise SystemExit(1)
