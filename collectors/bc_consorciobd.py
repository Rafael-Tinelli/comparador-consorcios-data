#!/usr/bin/env python3
"""
Coletor do ConsorcioBD (BCB) via portal oficial de Dados Abertos (CKAN API).

Estratégia desta versão:
- abandona scraping da SPA pública;
- consulta o portal oficial de dados abertos via API CKAN;
- pesquisa datasets/recursos de consórcio por termos configuráveis;
- seleciona o melhor recurso por score;
- baixa o recurso escolhido;
- grava latest_source.bin, latest.json, consorciobd_catalog.json e runtime;
- só considera sucesso quando mode_used = "ckan-api-resource-download".
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
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
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
    github_output = os.environ.get("GITHUB_OUTPUT")
    if not github_output:
        return
    with open(github_output, "a", encoding="utf-8") as fh:
        fh.write(f"{name}={value}\n")


def stable_json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_of(data: Any) -> str:
    return hashlib.sha256(stable_json_dumps(data).encode("utf-8")).hexdigest()


def sha256_of_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalize_spaces(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    return text or None


def normalize_match_text(value: Optional[str]) -> str:
    text = normalize_spaces(value) or ""
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    return text


def preview_text(text: str, limit: int = 400) -> str:
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

    if binary.startswith(b"PK\x03\x04"):
        try:
            with zipfile.ZipFile(io.BytesIO(binary)) as zf:
                members = zf.namelist()
                info["archive_members"] = members

                lower_members = [name.lower() for name in members]
                if any(name.endswith(".csv") for name in lower_members):
                    info["detected_format"] = "zip_with_csv"
                    return info
                if any(name.endswith((".xlsx", ".xlsm", ".xltx", ".xltm", ".xls")) for name in lower_members):
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


def get_extension_from_url(url: str) -> Optional[str]:
    path = urlparse(url).path.lower()
    for ext in (".zip", ".csv", ".xlsx", ".xls", ".json"):
        if path.endswith(ext):
            return ext
    return None


def ckan_package_search(
    session: requests.Session,
    base_url: str,
    endpoint: str,
    query: str,
    rows: int,
    headers: Dict[str, str],
) -> Dict[str, Any]:
    url = urljoin(base_url.rstrip("/") + "/", endpoint.lstrip("/"))
    response = session.get(
        url,
        params={"q": query, "rows": rows},
        headers=headers,
        timeout=getattr(session, "request_timeout", 60),
    )
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, dict) or not payload.get("success"):
        raise ValueError(f"Resposta inválida do CKAN package_search para query={query!r}.")

    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError(f"Resposta CKAN sem campo result para query={query!r}.")

    return result


def score_text(text: str, preferred_terms: List[str], exclude_terms: List[str]) -> int:
    score = 0
    text_norm = normalize_match_text(text)

    for term in preferred_terms:
        term_norm = normalize_match_text(term)
        if term_norm and term_norm in text_norm:
            score += 50

    for term in exclude_terms:
        term_norm = normalize_match_text(term)
        if term_norm and term_norm in text_norm:
            score -= 80

    return score


def format_rank(fmt: Optional[str], preferred_formats: List[str]) -> int:
    if not fmt:
        return 0
    fmt_upper = fmt.upper()
    for index, item in enumerate(preferred_formats):
        if fmt_upper == item.upper():
            return max(1, len(preferred_formats) - index)
    return 0


def build_package_summary(pkg: Dict[str, Any]) -> Dict[str, Any]:
    org = pkg.get("organization") or {}
    return {
        "name": pkg.get("name"),
        "title": pkg.get("title"),
        "organization_name": org.get("name"),
        "organization_title": org.get("title"),
        "metadata_modified": pkg.get("metadata_modified"),
        "resources_count": len(pkg.get("resources") or []),
    }


def build_resource_candidate(
    package: Dict[str, Any],
    resource: Dict[str, Any],
    cfg: Dict[str, Any],
    package_query: str,
) -> Optional[Dict[str, Any]]:
    url = normalize_spaces(resource.get("url"))
    if not url:
        return None

    resource_format = normalize_spaces(resource.get("format")) or ""
    ext = get_extension_from_url(url)

    allowed_formats = [x.upper() for x in cfg.get("allowed_resource_formats", [])]
    if resource_format:
        if allowed_formats and resource_format.upper() not in allowed_formats and not ext:
            return None
    elif allowed_formats and ext is None:
        return None

    package_title = normalize_spaces(package.get("title")) or ""
    package_name = normalize_spaces(package.get("name")) or ""
    package_notes = normalize_spaces(package.get("notes")) or ""
    org = package.get("organization") or {}
    org_name = normalize_spaces(org.get("name")) or ""
    org_title = normalize_spaces(org.get("title")) or ""

    resource_name = normalize_spaces(resource.get("name")) or ""
    resource_desc = normalize_spaces(resource.get("description")) or ""

    package_blob = " | ".join([package_title, package_name, package_notes, org_name, org_title])
    resource_blob = " | ".join([resource_name, resource_desc, url, resource_format])

    score = 0
    score += score_text(
        package_blob,
        cfg.get("preferred_package_terms", []),
        cfg.get("exclude_package_terms", []),
    )
    score += score_text(
        resource_blob,
        cfg.get("preferred_resource_terms", []),
        cfg.get("exclude_resource_terms", []),
    )
    score += format_rank(resource_format or ext or "", cfg.get("preferred_resource_formats", [])) * 10

    if normalize_match_text(package_query) in normalize_match_text(package_blob + " " + resource_blob):
        score += 15

    if ext in (".xlsx", ".zip", ".csv", ".xls"):
        score += 10

    candidate = {
        "score": score,
        "query": package_query,
        "package": build_package_summary(package),
        "resource": {
            "id": resource.get("id"),
            "name": resource_name,
            "description": resource_desc,
            "format": resource_format,
            "url": url,
            "extension": ext,
            "last_modified": resource.get("last_modified"),
            "created": resource.get("created"),
        },
    }
    return candidate


def dedupe_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[str, Dict[str, Any]] = {}
    for item in candidates:
        key = item["resource"]["url"]
        current = deduped.get(key)
        if current is None or item["score"] > current["score"]:
            deduped[key] = item

    final = list(deduped.values())
    final.sort(
        key=lambda x: (
            x["score"],
            x["package"].get("metadata_modified") or "",
            x["resource"].get("last_modified") or "",
            x["resource"].get("created") or "",
        ),
        reverse=True,
    )
    return final


def download_selected_resource(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
) -> Tuple[bytes, Dict[str, Any]]:
    response = session.get(
        url,
        headers=headers,
        timeout=getattr(session, "request_timeout", 180),
        allow_redirects=True,
    )
    response.raise_for_status()

    binary = response.content
    if not binary:
        raise ValueError("O recurso selecionado retornou conteúdo vazio.")

    meta = {
        "status_code": response.status_code,
        "final_url": response.url,
        "content_type": response.headers.get("Content-Type"),
        "content_length": response.headers.get("Content-Length"),
    }
    return binary, meta


def read_existing_source_sha(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    return sha256_of_bytes(path.read_bytes())


def main() -> int:
    parser = argparse.ArgumentParser(description="Coletor do ConsorcioBD via API CKAN.")
    parser.add_argument("--config", required=True, help="Caminho para config/sources.json")
    parser.add_argument("--source", default="bc_consorciobd", help="Nome da fonte em config/sources.json")
    args = parser.parse_args()

    config = load_json(Path(args.config))
    source_cfg = get_source_config(config, args.source)
    defaults = config.get("defaults", {})

    raw_dir = Path(source_cfg["storage"]["raw_dir"])
    stage_dir = Path(source_cfg["storage"]["stage_dir"])
    runtime_dir = Path("data/runtime")

    raw_file = raw_dir / "latest.json"
    raw_binary_file = raw_dir / "latest_source.bin"
    stage_file = stage_dir / "consorciobd_catalog.json"
    runtime_file = runtime_dir / "bc_consorciobd.json"

    raw_dir.mkdir(parents=True, exist_ok=True)
    stage_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    collected_at = utc_now_iso()
    mode_used = "ckan-api-search-failed"

    if not source_cfg.get("enabled", False):
        runtime_payload = {
            "source": args.source,
            "last_checked_at": collected_at,
            "changed": False,
            "mode_used": "disabled",
            "raw_file": str(raw_file),
            "raw_binary_file": str(raw_binary_file),
            "stage_file": str(stage_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("mode_used", "disabled")
        print(f"Fonte '{args.source}' está desabilitada.")
        return 0

    api_cfg = source_cfg["catalog_api"]
    user_agent = defaults.get(
        "user_agent",
        "comparador-consorcios-data/1.0 (+https://sanida.com.br/financas/consorcio/)",
    )
    timeout_seconds = int(defaults.get("timeout_seconds", 60))
    session = build_session(timeout_seconds=timeout_seconds)

    headers = {
        "Accept": "application/json",
        "User-Agent": user_agent,
    }

    search_terms = api_cfg.get("search_terms", [])
    if not search_terms:
        raise ValueError("catalog_api.search_terms não pode estar vazio em config/sources.json.")

    rows = int(api_cfg.get("rows", 100))
    package_summaries: List[Dict[str, Any]] = []
    resource_candidates: List[Dict[str, Any]] = []
    query_debug: List[Dict[str, Any]] = []

    try:
        for query in search_terms:
            result = ckan_package_search(
                session=session,
                base_url=api_cfg["base_url"],
                endpoint=api_cfg["package_search_endpoint"],
                query=query,
                rows=rows,
                headers=headers,
            )

            results = result.get("results") or []
            if not isinstance(results, list):
                results = []

            query_debug.append(
                {
                    "query": query,
                    "count": result.get("count"),
                    "returned": len(results),
                }
            )

            for pkg in results:
                if not isinstance(pkg, dict):
                    continue

                package_summaries.append(build_package_summary(pkg))

                resources = pkg.get("resources") or []
                if not isinstance(resources, list):
                    continue

                for resource in resources:
                    if not isinstance(resource, dict):
                        continue
                    candidate = build_resource_candidate(
                        package=pkg,
                        resource=resource,
                        cfg=api_cfg,
                        package_query=query,
                    )
                    if candidate:
                        resource_candidates.append(candidate)

        resource_candidates = dedupe_candidates(resource_candidates)
        package_summaries = package_summaries[: api_cfg.get("package_summary_limit", 50)]

        if not resource_candidates:
            runtime_payload = {
                "source": args.source,
                "last_checked_at": collected_at,
                "changed": False,
                "mode_used": mode_used,
                "query_debug": query_debug,
                "package_summaries": package_summaries,
                "candidate_count": 0,
                "raw_file": str(raw_file),
                "raw_binary_file": str(raw_binary_file),
                "stage_file": str(stage_file),
            }
            dump_json(runtime_file, runtime_payload)
            append_github_output("changed", "false")
            append_github_output("records", "0")
            append_github_output("mode_used", mode_used)
            raise ValueError("Nenhum recurso compatível foi encontrado via CKAN API para o ConsorcioBD.")

        selected = resource_candidates[0]
        binary, download_meta = download_selected_resource(
            session=session,
            url=selected["resource"]["url"],
            headers={"Accept": "*/*", "User-Agent": user_agent},
        )

        source_sha = sha256_of_bytes(binary)
        previous_source_sha = read_existing_source_sha(raw_binary_file)
        changed = previous_source_sha != source_sha
        mode_used = "ckan-api-resource-download"

        inspection = inspect_binary_payload(binary, download_meta.get("content_type"))

        selected_resource = {
            "selected_candidate": selected,
            "download_meta": download_meta,
            "sha256": source_sha,
            "inspection": inspection,
        }

        raw_payload = {
            "metadata": {
                "source": args.source,
                "collector": "collectors/bc_consorciobd.py",
                "collected_at": collected_at,
                "mode_used": mode_used,
                "query_count": len(search_terms),
                "candidate_count": len(resource_candidates),
                "resource_sha256": source_sha,
            },
            "query_debug": query_debug,
            "selected_resource": selected_resource,
            "top_candidates": resource_candidates[: api_cfg.get("candidate_limit", 50)],
        }

        stage_payload = {
            "metadata": {
                "source": args.source,
                "collector": "collectors/bc_consorciobd.py",
                "collected_at": collected_at,
                "mode_used": mode_used,
                "query_count": len(search_terms),
                "candidate_count": len(resource_candidates),
                "resource_sha256": source_sha,
            },
            "selected_resource": selected_resource,
            "top_candidates": resource_candidates[: api_cfg.get("candidate_limit", 50)],
            "query_debug": query_debug,
        }

        runtime_payload = {
            "source": args.source,
            "last_checked_at": collected_at,
            "last_changed_at": collected_at if changed else None,
            "changed": changed,
            "mode_used": mode_used,
            "candidate_count": len(resource_candidates),
            "query_debug": query_debug,
            "selected_url": selected["resource"]["url"],
            "final_url": download_meta.get("final_url"),
            "resource_sha256": source_sha,
            "raw_file": str(raw_file),
            "raw_binary_file": str(raw_binary_file),
            "stage_file": str(stage_file),
        }

        if changed:
            raw_binary_file.write_bytes(binary)
            dump_json(raw_file, raw_payload)
            dump_json(stage_file, stage_payload)

        dump_json(runtime_file, runtime_payload)

        append_github_output("changed", "true" if changed else "false")
        append_github_output("records", str(len(resource_candidates)))
        append_github_output("stage_hash", sha256_of(stage_payload))
        append_github_output("mode_used", mode_used)

        print(
            json.dumps(
                {
                    "source": args.source,
                    "changed": changed,
                    "records": len(resource_candidates),
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

    except requests.HTTPError as exc:
        runtime_payload = {
            "source": args.source,
            "last_checked_at": collected_at,
            "changed": False,
            "mode_used": mode_used,
            "query_debug": query_debug,
            "error": f"Erro HTTP ao coletar ConsorcioBD via CKAN API: {exc}",
            "raw_file": str(raw_file),
            "raw_binary_file": str(raw_binary_file),
            "stage_file": str(stage_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("mode_used", mode_used)
        print(f"Erro HTTP ao coletar ConsorcioBD via CKAN API: {exc}", file=sys.stderr)
        return 1

    except Exception as exc:
        runtime_payload = {
            "source": args.source,
            "last_checked_at": collected_at,
            "changed": False,
            "mode_used": mode_used,
            "query_debug": query_debug,
            "error": str(exc),
            "raw_file": str(raw_file),
            "raw_binary_file": str(raw_binary_file),
            "stage_file": str(stage_file),
        }
        dump_json(runtime_file, runtime_payload)
        append_github_output("changed", "false")
        append_github_output("records", "0")
        append_github_output("mode_used", mode_used)
        print(f"Falha no coletor bc_consorciobd: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
