#!/usr/bin/env python3
"""
Coletor do ConsorcioBD (BCB) por probing de URLs diretas de conteúdo.

Estratégia desta versão:
- não depende da página SPA como fonte primária de descoberta;
- tenta URLs diretas parametrizadas por competência;
- usa HEAD e, quando necessário, GET de prova;
- baixa o recurso escolhido e grava latest_source.bin;
- salva runtime mesmo em falha;
- usa a página oficial apenas como diagnóstico auxiliar.

Sucesso real:
- mode_used = "direct-content-download"
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
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


def append_github_output(name: str, value: str) -> None:
    github_output = Path(sys.argv[0]).parent  # placeholder to satisfy type checker
    env_path = Path(
        __import__("os").environ.get("GITHUB_OUTPUT", "")
    )
    if not env_path or str(env_path) == ".":
        return
    with env_path.open("a", encoding="utf-8") as fh:
        fh.write(f"{name}={value}\n")


def stable_json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_of(data: Any) -> str:
    import hashlib
    return hashlib.sha256(stable_json_dumps(data).encode("utf-8")).hexdigest()


def sha256_of_bytes(data: bytes) -> str:
    import hashlib
    return hashlib.sha256(data).hexdigest()


def normalize_spaces(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    return text or None


def preview_text(text: str, limit: int = 500) -> str:
    compact = " ".join((text or "").split())
    return compact[:limit]


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


def month_iter(reference: date, months_back: int) -> List[Tuple[int, int]]:
    items: List[Tuple[int, int]] = []
    year = reference.year
    month = reference.month

    for _ in range(months_back):
        items.append((year, month))
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return items


def render_pattern(pattern: str, base_url: str, year: int, month: int) -> str:
    yyyymm = f"{year}{month:02d}"
    return pattern.format(
        base_url=base_url.rstrip("/"),
        yyyy=f"{year}",
        mm=f"{month:02d}",
        yyyymm=yyyymm,
    )


def detect_csv_delimiter(text: str) -> str:
    sample = text[:10000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
        return dialect.delimiter
    except Exception:
        return ";" if sample.count(";") > sample.count(",") else ","


def inspect_binary_payload(binary: bytes, content_type: Optional[str]) -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "detected_format": "binary",
        "content_type": content_type,
        "size_bytes": len(binary),
    }

    # ZIP / XLSX
    if binary.startswith(b"PK\x03\x04"):
        try:
            with zipfile.ZipFile(io.BytesIO(binary)) as zf:
                members = zf.namelist()
                info["archive_members"] = members

                lower_members = [name.lower() for name in members]
                if any(name.endswith(".csv") for name in lower_members):
                    info["detected_format"] = "zip_with_csv"
                    return info
                if any(name.endswith((".xlsx", ".xlsm", ".xltx", ".xltm")) for name in lower_members):
                    info["detected_format"] = "zip_with_excel"
                    return info

                try:
                    wb = load_workbook(io.BytesIO(binary), read_only=True, data_only=True)
                    info["detected_format"] = "xlsx"
                    info["workbook_sheets"] = list(wb.sheetnames)
                    wb.close()
                    return info
                except Exception:
                    info["detected_format"] = "zip"
                    return info
        except Exception:
            pass

        try:
            wb = load_workbook(io.BytesIO(binary), read_only=True, data_only=True)
            info["detected_format"] = "xlsx"
            info["workbook_sheets"] = list(wb.sheetnames)
            wb.close()
            return info
        except Exception:
            pass

    # CSV puro
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            text = binary.decode(encoding)
            delimiter = detect_csv_delimiter(text)
            reader = csv.reader(io.StringIO(text), delimiter=delimiter)
            header = next(reader, [])
            if header:
                info["detected_format"] = "csv"
                info["csv_encoding"] = encoding
                info["csv_delimiter"] = delimiter
                info["csv_header"] = header[:50]
                return info
        except Exception:
            continue

    return info


def response_looks_like_file(resp: requests.Response) -> bool:
    content_type = (resp.headers.get("Content-Type") or "").lower()
    if "text/html" in content_type:
        return False
    if resp.status_code != 200:
        return False
    return True


def probe_head(session: requests.Session, url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    try:
        resp = session.head(
            url,
            headers=headers,
            timeout=getattr(session, "request_timeout", 60),
            allow_redirects=True,
        )
        return {
            "method": "HEAD",
            "ok": response_looks_like_file(resp),
            "status_code": resp.status_code,
            "final_url": resp.url,
            "content_type": resp.headers.get("Content-Type"),
            "content_length": resp.headers.get("Content-Length"),
        }
    except Exception as exc:
        return {
            "method": "HEAD",
            "ok": False,
            "status_code": None,
            "final_url": url,
            "content_type": None,
            "content_length": None,
            "error": str(exc),
        }


def probe_get(session: requests.Session, url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    try:
        resp = session.get(
            url,
            headers=headers,
            timeout=getattr(session, "request_timeout", 90),
            allow_redirects=True,
            stream=True,
        )
        first_chunk = b""
        try:
            for chunk in resp.iter_content(chunk_size=4096):
                if chunk:
                    first_chunk = chunk
                    break
        finally:
            resp.close()

        content_type = (resp.headers.get("Content-Type") or "").lower()
        looks_file = resp.status_code == 200 and "text/html" not in content_type

        return {
            "method": "GET",
            "ok": looks_file,
            "status_code": resp.status_code,
            "final_url": resp.url,
            "content_type": resp.headers.get("Content-Type"),
            "content_length": resp.headers.get("Content-Length"),
            "first_chunk_sha256": sha256_of_bytes(first_chunk) if first_chunk else None,
            "first_chunk_preview_hex": first_chunk[:32].hex() if first_chunk else None,
        }
    except Exception as exc:
        return {
            "method": "GET",
            "ok": False,
            "status_code": None,
            "final_url": url,
            "content_type": None,
            "content_length": None,
            "error": str(exc),
        }


def build_probe_candidates(
    direct_cfg: Dict[str, Any],
    reference_date: date,
) -> List[Dict[str, Any]]:
    base_url = direct_cfg["base_url"]
    patterns = direct_cfg["candidate_patterns"]
    months_back = int(direct_cfg.get("probe_months_back", 12))

    candidates: List[Dict[str, Any]] = []
    index = 0

    for year, month in month_iter(reference_date, months_back):
        for pattern in patterns:
            index += 1
            url = render_pattern(pattern, base_url, year, month)
            candidates.append(
                {
                    "index": index,
                    "year": year,
                    "month": month,
                    "yyyymm": f"{year}{month:02d}",
                    "pattern": pattern,
                    "url": url,
                }
            )

    return candidates


def read_existing_source_sha(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    return sha256_of_bytes(path.read_bytes())


def fetch_diagnostic_page(
    session: requests.Session,
    page_url: str,
    headers: Dict[str, str],
    snapshot_file: Path,
) -> Dict[str, Any]:
    try:
        resp = session.get(
            page_url,
            headers=headers,
            timeout=getattr(session, "request_timeout", 60),
            allow_redirects=True,
        )
        html = resp.text or ""
        snapshot_file.write_text(html, encoding="utf-8")
        soup = BeautifulSoup(html, "lxml")
        script_srcs = [normalize_spaces(tag.get("src")) for tag in soup.find_all("script", src=True)]
        return {
            "status_code": resp.status_code,
            "final_url": resp.url,
            "content_type": resp.headers.get("Content-Type"),
            "html_preview": preview_text(html),
            "script_srcs": [src for src in script_srcs if src][:20],
            "snapshot_file": str(snapshot_file),
        }
    except Exception as exc:
        return {
            "error": str(exc),
            "snapshot_file": str(snapshot_file),
        }


def main() -> int:
    import os

    parser = argparse.ArgumentParser(description="Coletor do ConsorcioBD por URLs diretas.")
    parser.add_argument("--config", required=True, help="Caminho para config/sources.json")
    parser.add_argument("--source", default="bc_consorciobd", help="Nome da fonte em config/sources.json")
    args = parser.parse_args()

    config_path = Path(args.config)
    config = load_json(config_path)
    source_cfg = get_source_config(config, args.source)
    defaults = config.get("defaults", {})

    raw_dir = Path(source_cfg["storage"]["raw_dir"])
    stage_dir = Path(source_cfg["storage"]["stage_dir"])
    runtime_dir = Path("data/runtime")

    raw_file = raw_dir / "latest.json"
    raw_binary_file = raw_dir / "latest_source.bin"
    stage_file = stage_dir / "consorciobd_catalog.json"
    runtime_file = runtime_dir / "bc_consorciobd.json"
    snapshot_file = raw_dir / "page_snapshot.html"

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
            "raw_binary_file": str(raw_binary_file),
            "stage_file": str(stage_file),
            "snapshot_file": str(snapshot_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("mode_used", "disabled")
        print(f"Fonte '{args.source}' está desabilitada.")
        return 0

    direct_cfg = source_cfg["direct_download"]
    diag_cfg = source_cfg.get("diagnostic_page", {})

    user_agent = defaults.get(
        "user_agent",
        "comparador-consorcios-data/1.0 (+https://sanida.com.br/financas/consorcio/)",
    )
    timeout_seconds = int(defaults.get("timeout_seconds", 60))
    session = build_session(timeout_seconds=timeout_seconds)

    head_headers = {
        "Accept": "*/*",
        "User-Agent": user_agent,
    }
    page_headers = {
        "Accept": "text/html,application/xhtml+xml",
        "User-Agent": user_agent,
    }

    reference_date = datetime.now(timezone.utc).date()
    candidates = build_probe_candidates(direct_cfg, reference_date)
    probe_results: List[Dict[str, Any]] = []

    selected_candidate: Optional[Dict[str, Any]] = None
    selected_probe: Optional[Dict[str, Any]] = None

    methods = [m.upper() for m in direct_cfg.get("http_methods", ["HEAD", "GET"])]

    for candidate in candidates:
        result_entry = {
            "candidate": candidate,
            "probes": [],
        }

        head_result: Optional[Dict[str, Any]] = None
        get_result: Optional[Dict[str, Any]] = None

        if "HEAD" in methods:
            head_result = probe_head(session, candidate["url"], head_headers)
            result_entry["probes"].append(head_result)

        head_ok = bool(head_result and head_result.get("ok"))

        if not head_ok and "GET" in methods:
            # GET de prova só entra quando HEAD não confirma
            get_result = probe_get(session, candidate["url"], head_headers)
            result_entry["probes"].append(get_result)

        get_ok = bool(get_result and get_result.get("ok"))

        probe_results.append(result_entry)

        if head_ok:
            selected_candidate = candidate
            selected_probe = head_result
            break

        if get_ok:
            selected_candidate = candidate
            selected_probe = get_result
            break

    if not selected_candidate:
        diagnostic_page = {}
        if diag_cfg.get("enabled", True):
            diagnostic_page = fetch_diagnostic_page(
                session=session,
                page_url=diag_cfg.get("page_url") or source_cfg["official_page_url"],
                headers=page_headers,
                snapshot_file=snapshot_file,
            )

        runtime_payload = {
            "source": args.source,
            "last_checked_at": collected_at,
            "changed": False,
            "mode_used": mode_used,
            "probe_count": len(probe_results),
            "probe_results": probe_results[:40],
            "diagnostic_page": diagnostic_page,
            "raw_file": str(raw_file),
            "raw_binary_file": str(raw_binary_file),
            "stage_file": str(stage_file),
            "snapshot_file": str(snapshot_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", str(len(probe_results)))
        append_github_output("mode_used", mode_used)
        raise ValueError(
            "Nenhuma URL direta homologável respondeu como arquivo válido para o ConsorcioBD. "
            f"Runtime salvo em {runtime_file}."
        )

    # Download completo do candidato escolhido
    download_resp = session.get(
        selected_candidate["url"],
        headers=head_headers,
        timeout=getattr(session, "request_timeout", 180),
        allow_redirects=True,
    )
    download_resp.raise_for_status()

    binary = download_resp.content
    if not binary:
        runtime_payload = {
            "source": args.source,
            "last_checked_at": collected_at,
            "changed": False,
            "mode_used": mode_used,
            "selected_candidate": selected_candidate,
            "selected_probe": selected_probe,
            "error": "O download final retornou corpo vazio.",
            "raw_file": str(raw_file),
            "raw_binary_file": str(raw_binary_file),
            "stage_file": str(stage_file),
            "snapshot_file": str(snapshot_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", str(len(probe_results)))
        append_github_output("mode_used", mode_used)
        raise ValueError("O recurso selecionado do ConsorcioBD retornou conteúdo vazio.")

    source_sha = sha256_of_bytes(binary)
    previous_source_sha = read_existing_source_sha(raw_binary_file)
    changed = previous_source_sha != source_sha
    mode_used = "direct-content-download"

    inspection = inspect_binary_payload(binary, download_resp.headers.get("Content-Type"))

    selected_resource = {
        "selected_candidate": selected_candidate,
        "selected_probe": selected_probe,
        "final_url": download_resp.url,
        "status_code": download_resp.status_code,
        "content_type": download_resp.headers.get("Content-Type"),
        "content_length": download_resp.headers.get("Content-Length"),
        "sha256": source_sha,
        "inspection": inspection,
    }

    raw_payload = {
        "metadata": {
            "source": args.source,
            "collector": "collectors/bc_consorciobd.py",
            "collected_at": collected_at,
            "mode_used": mode_used,
            "probe_count": len(probe_results),
            "resource_sha256": source_sha,
        },
        "selected_resource": selected_resource,
        "probe_results": probe_results,
    }

    stage_payload = {
        "metadata": {
            "source": args.source,
            "collector": "collectors/bc_consorciobd.py",
            "collected_at": collected_at,
            "mode_used": mode_used,
            "probe_count": len(probe_results),
            "resource_sha256": source_sha,
        },
        "resource": selected_resource,
        "probe_results": probe_results,
    }

    runtime_payload = {
        "source": args.source,
        "last_checked_at": collected_at,
        "last_changed_at": collected_at if changed else None,
        "changed": changed,
        "mode_used": mode_used,
        "probe_count": len(probe_results),
        "resource_sha256": source_sha,
        "selected_url": selected_candidate["url"],
        "final_url": download_resp.url,
        "raw_file": str(raw_file),
        "raw_binary_file": str(raw_binary_file),
        "stage_file": str(stage_file),
        "snapshot_file": str(snapshot_file),
    }

    if changed:
        raw_binary_file.write_bytes(binary)
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
                "raw_binary_file": str(raw_binary_file),
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
        print(f"Erro HTTP ao coletar ConsorcioBD: {exc}", file=sys.stderr)
        raise SystemExit(1)
    except Exception as exc:
        print(f"Falha no coletor bc_consorciobd: {exc}", file=sys.stderr)
        raise SystemExit(1)
