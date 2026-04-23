#!/usr/bin/env python3
"""
Coletor do boletim mensal da ABAC.

Estratégia:
- consulta a página oficial de boletins da ABAC;
- extrai os links "BAIXAR" associados a mês/ano;
- ordena os candidatos do mais recente para o mais antigo;
- valida os links por HEAD/GET;
- baixa o PDF mais recente válido;
- grava raw, stage e runtime;
- só considera sucesso quando mode_used = "materials-page-download".
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


MONTHS = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "março": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}


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


def preview_text(text: str, limit: int = 350) -> str:
    compact = " ".join((text or "").split())
    return compact[:limit]


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


def candidate_looks_valid_from_headers(
    *,
    status_code: Optional[int],
    final_url: str,
    content_type: Optional[str],
    content_length: Optional[str],
    min_size_bytes: int,
) -> bool:
    if status_code != 200:
        return False

    final_url_l = final_url.lower()
    ct = (content_type or "").lower()

    if not (final_url_l.endswith(".pdf") or "application/pdf" in ct):
        return False

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
            final_url=result["final_url"],
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
            final_url=result["final_url"],
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


def parse_period_label(text: str) -> Optional[Tuple[int, int, str]]:
    if not text:
        return None

    match = re.search(
        r"(?i)\b(janeiro|fevereiro|março|marco|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)\s*/\s*(\d{2,4})\b",
        text,
    )
    if not match:
        return None

    month_name = match.group(1).lower()
    year_raw = match.group(2)

    month = MONTHS.get(month_name)
    if month is None:
        return None

    year = int(year_raw)
    if year < 100:
        year += 2000

    return year, month, f"{match.group(1)}/{match.group(2)}"


def extract_candidates(
    html: str,
    page_url: str,
    allowed_host_substrings: List[str],
    link_text_contains: str,
) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    candidates: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = normalize_spaces(anchor.get("href"))
        if not href:
            continue

        anchor_text = normalize_spaces(anchor.get_text(" ", strip=True)) or ""
        if link_text_contains.lower() not in anchor_text.lower():
            continue

        resolved_url = urljoin(page_url, href)
        if not any(host_part.lower() in resolved_url.lower() for host_part in allowed_host_substrings):
            continue

        if resolved_url in seen_urls:
            continue
        seen_urls.add(resolved_url)

        period_info = None
        for prev_text in anchor.find_all_previous(string=True, limit=30):
            cleaned = normalize_spaces(prev_text)
            if not cleaned:
                continue
            period_info = parse_period_label(cleaned)
            if period_info:
                break

        if not period_info:
            continue

        year, month, period_label = period_info

        candidates.append(
            {
                "period_label": period_label,
                "year": year,
                "month": month,
                "url": resolved_url,
                "link_text": anchor_text,
            }
        )

    candidates.sort(key=lambda x: (x["year"], x["month"]), reverse=True)
    return candidates


def download_pdf(session: requests.Session, url: str, headers: Dict[str, str]) -> Tuple[bytes, Dict[str, Any]]:
    response = session.get(
        url,
        headers=headers,
        timeout=getattr(session, "request_timeout", 180),
        allow_redirects=True,
    )
    response.raise_for_status()

    binary = response.content
    if not binary:
        raise ValueError("O download final do boletim retornou conteúdo vazio.")

    meta = {
        "status_code": response.status_code,
        "final_url": response.url,
        "content_type": response.headers.get("Content-Type"),
        "content_length": response.headers.get("Content-Length"),
        "last_modified": response.headers.get("Last-Modified"),
        "etag": response.headers.get("ETag"),
    }
    return binary, meta


def read_existing_source_sha(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    return sha256_of_bytes(path.read_bytes())


def main() -> int:
    parser = argparse.ArgumentParser(description="Coletor do boletim mensal da ABAC.")
    parser.add_argument("--config", required=True, help="Caminho para config/sources.json")
    parser.add_argument("--source", default="abac_boletim", help="Nome da fonte em config/sources.json")
    args = parser.parse_args()

    config = load_json(Path(args.config))
    source_cfg = get_source_config(config, args.source)
    defaults = config.get("defaults", {})

    raw_dir = Path(source_cfg["storage"]["raw_dir"])
    stage_dir = Path(source_cfg["storage"]["stage_dir"])
    runtime_dir = Path("data/runtime")

    raw_file = raw_dir / "latest.json"
    raw_source_file = raw_dir / "latest_source.pdf"
    stage_file = stage_dir / "abac_boletim.json"
    runtime_file = runtime_dir / "abac_boletim.json"

    raw_dir.mkdir(parents=True, exist_ok=True)
    stage_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    collected_at = utc_now_iso()
    mode_used = "materials-page-failed"

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

    discovery_cfg = source_cfg["discovery"]
    page_url = discovery_cfg["page_url"]
    allowed_host_substrings = discovery_cfg.get("allowed_host_substrings", [])
    link_text_contains = discovery_cfg.get("link_text_contains", "BAIXAR")
    probe_top_candidates = int(discovery_cfg.get("probe_top_candidates", 8))
    min_size_bytes = int(discovery_cfg.get("min_size_bytes", 50000))
    http_methods = [m.upper() for m in discovery_cfg.get("http_methods", ["HEAD", "GET"])]

    user_agent = defaults.get(
        "user_agent",
        "comparador-consorcios-data/1.0 (+https://sanida.com.br/financas/consorcio/)",
    )
    timeout_seconds = int(defaults.get("timeout_seconds", 60))
    session = build_session(timeout_seconds=timeout_seconds)

    page_headers = {
        "Accept": "text/html,application/xhtml+xml",
        "User-Agent": user_agent,
    }
    download_headers = {
        "Accept": "*/*",
        "User-Agent": user_agent,
        "Referer": page_url,
    }

    try:
        page_response = session.get(
            page_url,
            headers=page_headers,
            timeout=getattr(session, "request_timeout", 60),
            allow_redirects=True,
        )
        page_response.raise_for_status()

        html = page_response.text or ""
        candidates = extract_candidates(
            html=html,
            page_url=page_url,
            allowed_host_substrings=allowed_host_substrings,
            link_text_contains=link_text_contains,
        )

        if not candidates:
            runtime_payload = {
                "source": args.source,
                "last_checked_at": collected_at,
                "changed": False,
                "mode_used": mode_used,
                "html_preview": preview_text(html),
                "candidate_count": 0,
                "raw_file": str(raw_file),
                "raw_source_file": str(raw_source_file),
                "stage_file": str(stage_file),
            }
            dump_json(runtime_file, runtime_payload)
            append_github_output("changed", "false")
            append_github_output("records", "0")
            append_github_output("mode_used", mode_used)
            raise ValueError("Nenhum boletim mensal da ABAC foi identificado na página de materiais.")

        probe_results: List[Dict[str, Any]] = []
        selected_candidate: Optional[Dict[str, Any]] = None
        selected_probe: Optional[Dict[str, Any]] = None

        for candidate in candidates[:probe_top_candidates]:
            probes: List[Dict[str, Any]] = []
            head_result: Optional[Dict[str, Any]] = None
            get_result: Optional[Dict[str, Any]] = None

            if "HEAD" in http_methods:
                head_result = probe_head(session, candidate["url"], download_headers, min_size_bytes)
                probes.append(head_result)

            if head_result and head_result.get("ok"):
                selected_candidate = candidate
                selected_probe = head_result
                probe_results.append({"candidate": candidate, "probes": probes})
                break

            if "GET" in http_methods:
                get_result = probe_get(session, candidate["url"], download_headers, min_size_bytes)
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
                "candidate_count": len(candidates),
                "probe_results": probe_results,
                "raw_file": str(raw_file),
                "raw_source_file": str(raw_source_file),
                "stage_file": str(stage_file),
            }
            dump_json(runtime_file, runtime_payload)
            append_github_output("changed", "false")
            append_github_output("records", str(len(candidates)))
            append_github_output("mode_used", mode_used)
            raise ValueError("Nenhum link de boletim ABAC respondeu como PDF válido.")

        binary, download_meta = download_pdf(
            session=session,
            url=selected_candidate["url"],
            headers=download_headers,
        )

        current_sha = sha256_of_bytes(binary)
        previous_sha = read_existing_source_sha(raw_source_file)
        changed = previous_sha != current_sha
        mode_used = "materials-page-download"

        raw_payload = {
            "metadata": {
                "source": args.source,
                "collector": "collectors/abac_boletim.py",
                "collected_at": collected_at,
                "mode_used": mode_used,
                "resource_sha256": current_sha,
                "candidate_count": len(candidates),
            },
            "selected": {
                "period_label": selected_candidate["period_label"],
                "year": selected_candidate["year"],
                "month": selected_candidate["month"],
                "download_url": selected_candidate["url"],
                "selected_probe": selected_probe,
                "download_meta": download_meta,
            },
            "top_candidates": candidates[:20],
            "probe_results": probe_results,
        }

        stage_payload = {
            "metadata": {
                "source": args.source,
                "collector": "collectors/abac_boletim.py",
                "collected_at": collected_at,
                "mode_used": mode_used,
                "resource_sha256": current_sha,
            },
            "selected": {
                "period_label": selected_candidate["period_label"],
                "year": selected_candidate["year"],
                "month": selected_candidate["month"],
                "download_url": selected_candidate["url"],
                "download_meta": download_meta,
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
            "selected_period_label": selected_candidate["period_label"],
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
            "error": f"Erro HTTP ao coletar boletim ABAC: {exc}",
            "raw_file": str(raw_file),
            "raw_source_file": str(raw_source_file),
            "stage_file": str(stage_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("mode_used", mode_used)
        print(f"Erro HTTP ao coletar boletim ABAC: {exc}", file=sys.stderr)
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
        print(f"Falha no coletor abac_boletim: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
