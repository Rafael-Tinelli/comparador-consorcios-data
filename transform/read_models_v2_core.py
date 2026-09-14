#!/usr/bin/env python3
"""Núcleo canônico de transformação dos read models do Comparador de Consórcios V2.

Este módulo contém somente parsing, normalização, construção e validação de dados.
Publicação, proveniência e interpretação relativa ficam nas camadas próprias.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import unicodedata
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Tuple

CONTRACTS = {
    "instituicoes": "instituicoes.v2",
    "administradoras": "administradoras.v2",
    "produtos": "produtos.v2",
    "rankings": "rankings.v2",
    "segmentos": "segmentos.v2",
    "comparacoes": "comparacoes.v2",
    "ofertas": "ofertas.v2",
}

SEGMENTS = {
    "1": ("imobiliario", "Consórcio de imóveis"),
    "2": ("veiculos_pesados_maquinas", "Veículos pesados, máquinas e equipamentos"),
    "3": ("veiculos", "Veículos leves"),
    "4": ("motos", "Motocicletas"),
    "5": ("outros_bens_moveis", "Outros bens móveis duráveis"),
    "6": ("servicos_turisticos", "Serviços turísticos"),
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def try_load_json(path: Path) -> Any:
    return load_json(path) if path.exists() else None


def dump_json_text(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def write_json_if_changed(path: Path, data: Any) -> bool:
    text = dump_json_text(data)
    path.parent.mkdir(parents=True, exist_ok=True)
    previous = path.read_text(encoding="utf-8") if path.exists() else None
    if previous == text:
        return False
    path.write_text(text, encoding="utf-8")
    return True


def normalize_key(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def normalize_text(value: Any) -> str:
    return normalize_key(value)


def pick_value(record: Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in record and record[key] not in (None, "", []):
            return record[key]
    return None


def root8(value: Any) -> Optional[str]:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) == 8:
        return digits
    if len(digits) == 14:
        return digits[:8]
    return None


def strict_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    text = str(value).strip().replace("\u00a0", "")
    if not text or not re.fullmatch(r"-?[0-9][0-9.,]*", text):
        return None
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    elif text.count(".") > 1:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def strict_int(value: Any) -> Optional[int]:
    number = strict_number(value)
    if number is None or not float(number).is_integer():
        return None
    return int(number)


def period_yyyymm(value: Any) -> Optional[str]:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) >= 6:
        candidate = digits[:6]
        if re.fullmatch(r"20\d{2}(0[1-9]|1[0-2])", candidate):
            return candidate
    return None


def availability(value: Any, *, reason_if_missing: str = "nao_informado_na_fonte") -> Dict[str, Any]:
    return {
        "value": value,
        "availability": "available" if value is not None else "unavailable",
        "reason": None if value is not None else reason_if_missing,
    }


def detect_csv_delimiter(text: str) -> str:
    try:
        return csv.Sniffer().sniff(text[:10000], delimiters=",;|\t").delimiter
    except Exception:
        return ";" if text[:10000].count(";") > text[:10000].count(",") else ","


def parse_csv_bytes(binary: bytes) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    text = None
    encoding_used = None
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            text = binary.decode(encoding)
            encoding_used = encoding
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise ValueError("Não foi possível decodificar CSV")
    delimiter = detect_csv_delimiter(text)
    rows = [
        {normalize_key(k): v for k, v in row.items()}
        for row in csv.DictReader(io.StringIO(text), delimiter=delimiter)
    ]
    return rows, {
        "encoding": encoding_used,
        "delimiter": delimiter,
        "row_count": len(rows),
        "headers": list(rows[0].keys()) if rows else [],
    }


def read_json_records(path: Path) -> List[Dict[str, Any]]:
    payload = load_json(path)
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("items", "records", "data", "instituicoes", "filiais"):
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
    raise ValueError(f"Schema não reconhecido em {path}")


def normalized_record(record: Dict[str, Any]) -> Dict[str, Any]:
    return {normalize_key(k): v for k, v in record.items()}


def load_registry(path: Path) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw in read_json_records(path):
        row = normalized_record(raw)
        name = pick_value(row, ["institution_name", "nome", "name", "noenti", "razao_social", "nome_fantasia"])
        root = root8(pick_value(row, ["cnpj_root", "cnpj", "cpf_cnpj", "cnpj14", "coenti", "institution_id"]))
        if not name or not root:
            continue
        if root in seen:
            raise ValueError(f"Cadastro com raiz duplicada: {root}")
        seen.add(root)
        items.append({
            "cnpj_root": root,
            "nome": str(name).strip(),
            "nome_normalizado": normalize_text(name),
            "status_detalhado": pick_value(row, ["status_text", "status", "situacao", "situacao_cadastral"]),
        })
    if len(items) < 100:
        raise ValueError(f"Cadastro crítico abaixo do mínimo: {len(items)}")
    return items


def load_branches(path: Path) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    by_root: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"filiais": 0, "ufs": set(), "municipios": set()})
    unresolved: List[Dict[str, Any]] = []
    for raw in read_json_records(path):
        row = normalized_record(raw)
        root = root8(pick_value(row, ["cnpj_root", "cnpj", "cpf_cnpj", "cnpj14", "cnpj_da_administradora", "coenti"]))
        if not root:
            unresolved.append({"nome": pick_value(row, ["institution_name", "nome", "administradora", "noenti"])})
            continue
        item = by_root[root]
        item["filiais"] += 1
        uf = pick_value(row, ["uf", "sigla_uf"])
        city = pick_value(row, ["city", "cidade", "municipio", "municipio_nome"])
        if uf:
            item["ufs"].add(str(uf).strip())
        if city:
            item["municipios"].add(str(city).strip())
    return {
        root: {
            "filiais": row["filiais"],
            "ufs": sorted(row["ufs"]),
            "municipios_count": len(row["municipios"]),
        }
        for root, row in by_root.items()
    }, unresolved


def load_rankings_v2(path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    rows, csv_meta = parse_csv_bytes(path.read_bytes())
    parsed: List[Dict[str, Any]] = []
    for row in rows:
        name = pick_value(row, ["administradora_de_consorcio", "nome_da_administradora", "instituicao", "nome", "administradora"])
        root = root8(pick_value(row, ["cnpj_ac", "cnpj", "cnpj_da_administradora"]))
        if not name or not root:
            continue
        index_value = strict_number(pick_value(row, ["indice", "indice_reclamacoes", "indice_de_reclamacoes"]))
        parsed.append({
            "cnpj_root": root,
            "nome": str(name).strip(),
            "nome_normalizado": normalize_text(name),
            "indice_bc": index_value,
            "indice_status": "divulgado" if index_value is not None else "nao_divulgado_pela_fonte",
            "posicao_oficial": strict_int(pick_value(row, ["posicao", "ranking", "classificacao"])),
            "reclamacoes_reguladas_procedentes": strict_int(pick_value(row, ["quantidade_de_reclamacoes_reguladas_procedentes", "reclamacoes_reguladas_procedentes"])),
            "reclamacoes_reguladas_outras": strict_int(pick_value(row, ["quantidade_de_reclamacoes_reguladas_outras", "reclamacoes_reguladas_outras"])),
            "reclamacoes_nao_reguladas": strict_int(pick_value(row, ["quantidade_de_reclamacoes_nao_reguladas", "reclamacoes_nao_reguladas"])),
            "reclamacoes_total": strict_int(pick_value(row, ["quantidade_total_de_reclamacoes", "total_de_reclamacoes", "reclamacoes"])),
            "consorciados_referencia": strict_int(pick_value(row, ["quantidade_de_clientes_consorciados", "clientes_consorciados", "clientes"])),
            "ano": strict_int(pick_value(row, ["ano"])),
            "semestre": str(pick_value(row, ["semestre"]) or "").strip() or None,
        })
    roots = [x["cnpj_root"] for x in parsed]
    if len(roots) != len(set(roots)):
        raise ValueError("Ranking com raiz duplicada no mesmo arquivo")
    return parsed, {"row_count": len(parsed), "csv": csv_meta}


def has_operational_signal(row: Dict[str, Any]) -> bool:
    values = (
        row.get("taxa_administracao_pct"),
        row.get("grupos_ativos"),
        row.get("cotas_ativas_em_dia"),
        row.get("contemplacoes_mes"),
        row.get("inadimplentes"),
    )
    return any(value is not None and value > 0 for value in values)


def load_monthly_v2(path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    consolidated: List[Tuple[str, Dict[str, Any]]] = []
    group_rows: List[Tuple[str, Dict[str, Any]]] = []
    with zipfile.ZipFile(path, "r") as zf:
        for member in zf.namelist():
            if not member.lower().endswith(".csv"):
                continue
            member_key = normalize_text(member)
            if "significado" in member_key or "layout" in member_key:
                continue
            rows, _ = parse_csv_bytes(zf.read(member))
            if "segmentos_consolidados" in member_key:
                consolidated.extend((member, row) for row in rows)
            elif "grupos" in member_key:
                group_rows.extend((member, row) for row in rows)
    if not consolidated:
        raise ValueError("ConsorcioBD mensal sem Segmentos_Consolidados")

    records: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    periods: set[str] = set()
    for member, row in consolidated:
        root = root8(pick_value(row, ["cnpj_da_administradora", "cnpj", "cnpj_ac"]))
        period = period_yyyymm(pick_value(row, ["data_base", "competencia"]))
        segment_code = re.sub(r"\D+", "", str(pick_value(row, ["codigo_do_segmento", "segmento", "codigo_segmento"]) or ""))
        if not root or not period or segment_code not in SEGMENTS:
            continue
        key = (root, period, segment_code)
        if key in records:
            raise ValueError(f"Chave duplicada no consolidado: {key}")
        periods.add(period)
        seg_key, seg_label = SEGMENTS[segment_code]
        n = strict_int(pick_value(row, ["quantidade_de_cotas_ativas_em_dia"]))
        inad_a = strict_int(pick_value(row, ["quantidade_de_cotas_ativas_contempladas_inadimplentes"]))
        inad_b = strict_int(pick_value(row, ["quantidade_de_cotas_ativas_nao_contempladas_inadimplentes"]))
        inad_total = inad_a + inad_b if inad_a is not None and inad_b is not None else None
        total_active = n + inad_total if n is not None and inad_total is not None else None
        inad_share = inad_total / total_active if total_active and inad_total is not None else None
        records[key] = {
            "cnpj_root": root,
            "nome_administradora": str(pick_value(row, ["nome_da_administradora", "administradora_de_consorcio", "administradora"]) or "").strip() or None,
            "competencia": period,
            "segmento_codigo": segment_code,
            "segmento": seg_key,
            "segmento_label": seg_label,
            "taxa_administracao_pct": strict_number(pick_value(row, ["taxa_de_administracao", "taxa_administracao"])),
            "grupos_ativos": strict_int(pick_value(row, ["quantidade_de_grupos_ativos"])),
            "cotas_ativas_em_dia": n,
            "contemplacoes_mes": strict_int(pick_value(row, ["quantidade_de_cotas_ativas_contempladas_no_mes"])),
            "inadimplentes": inad_total,
            "cotas_ativas_total_calculado": total_active,
            "inadimplencia_participacao": round(inad_share, 8) if inad_share is not None else None,
            "source_member": member,
        }

    if not periods:
        raise ValueError("ConsorcioBD mensal sem competência válida")
    latest = max(periods)
    latest_all = [record for (_root, period, _segment), record in records.items() if period == latest]
    active = [record for record in latest_all if has_operational_signal(record)]
    active_roots = {x["cnpj_root"] for x in active}
    if len(active_roots) < 100:
        raise ValueError(f"Cobertura operacional mensal crítica abaixo do mínimo: {len(active_roots)}")

    group_acc: Dict[Tuple[str, str, str], Dict[str, float]] = defaultdict(lambda: {
        "prazo_total": 0.0,
        "prazo_weight": 0.0,
        "credito_total": 0.0,
        "credito_weight": 0.0,
        "rows": 0.0,
    })
    for _member, row in group_rows:
        root = root8(pick_value(row, ["cnpj_da_administradora", "cnpj", "cnpj_ac"]))
        period = period_yyyymm(pick_value(row, ["data_base", "competencia"]))
        segment_code = re.sub(r"\D+", "", str(pick_value(row, ["codigo_do_segmento", "segmento", "codigo_segmento"]) or ""))
        if not root or period != latest or segment_code not in SEGMENTS:
            continue
        weight = strict_number(pick_value(row, ["quantidade_de_cotas_ativas_em_dia"]))
        weight = weight if weight is not None and weight > 0 else 1.0
        prazo = strict_number(pick_value(row, ["prazo_do_grupo_em_meses", "prazo_meses", "prazo"]))
        credito = strict_number(pick_value(row, ["valor_medio_do_bem", "valor_credito", "valor_medio"]))
        acc = group_acc[(root, period, segment_code)]
        acc["rows"] += 1
        if prazo is not None:
            acc["prazo_total"] += prazo * weight
            acc["prazo_weight"] += weight
        if credito is not None:
            acc["credito_total"] += credito * weight
            acc["credito_weight"] += weight

    for record in active:
        acc = group_acc.get((record["cnpj_root"], latest, record["segmento_codigo"]))
        record["detalhe_grupos"] = {
            "disponivel": bool(acc),
            "grupos_linhas": int(acc["rows"]) if acc else 0,
            "prazo_medio_grupos_meses": round(acc["prazo_total"] / acc["prazo_weight"], 4) if acc and acc["prazo_weight"] > 0 else None,
            "valor_medio_bem_grupos": round(acc["credito_total"] / acc["credito_weight"], 2) if acc and acc["credito_weight"] > 0 else None,
            "nota": "Detalhes por grupo servem apenas a prazo/crédito; estoques, fluxos e taxa vêm do consolidado.",
        }

    active.sort(key=lambda x: (x["cnpj_root"], x["segmento_codigo"]))
    return active, {
        "competencia": latest,
        "periodos_encontrados": sorted(periods),
        "consolidated_rows_latest": len(latest_all),
        "operational_rows_latest": len(active),
        "consolidated_roots_latest": len({x["cnpj_root"] for x in latest_all}),
        "operational_roots_latest": len(active_roots),
        "segmentos": sorted({x["segmento_codigo"] for x in active}),
    }


def sum_complete(rows: List[Dict[str, Any]], field: str) -> Optional[int]:
    if not rows:
        return None
    values = [row.get(field) for row in rows]
    if any(value is None for value in values):
        return None
    return int(sum(values))


def source_fingerprint(paths: Iterable[Path], version: str) -> str:
    h = hashlib.sha256(version.encode("utf-8"))
    for path in paths:
        h.update(str(path).encode("utf-8"))
        h.update(path.read_bytes())
    return h.hexdigest()


def stable_generated_at(meta_path: Path, fingerprint: str) -> str:
    previous = try_load_json(meta_path)
    if isinstance(previous, dict) and previous.get("source_fingerprint") == fingerprint and previous.get("generated_at"):
        return str(previous["generated_at"])
    return utc_now_iso()


def build_models(
    registry: List[Dict[str, Any]],
    branches: Dict[str, Dict[str, Any]],
    rankings: List[Dict[str, Any]],
    products: List[Dict[str, Any]],
    generated_at: str,
) -> Dict[str, Any]:
    registry_roots = {x["cnpj_root"] for x in registry}
    names = {x["cnpj_root"]: x["nome"] for x in registry}
    products_by_root: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for product in products:
        products_by_root[product["cnpj_root"]].append(product)
    rankings_by_root = {x["cnpj_root"]: x for x in rankings}

    institutions: List[Dict[str, Any]] = []
    admins: List[Dict[str, Any]] = []
    for inst in sorted(registry, key=lambda x: x["nome"]):
        root = inst["cnpj_root"]
        ops = products_by_root.get(root, [])
        rep = rankings_by_root.get(root)
        branch = branches.get(root, {"filiais": 0, "ufs": [], "municipios_count": 0})
        institutions.append({**inst, "cadastro_atual_bcb": True, "presenca_cadastrada": branch})

        total_groups = sum_complete(ops, "grupos_ativos")
        total_n = sum_complete(ops, "cotas_ativas_em_dia")
        total_contempl = sum_complete(ops, "contemplacoes_mes")
        total_inad = sum_complete(ops, "inadimplentes")
        total_active = total_n + total_inad if total_n is not None and total_inad is not None else None
        delinquency_share = total_inad / total_active if total_active and total_inad is not None else None

        portfolio = []
        for p in sorted(ops, key=lambda x: int(x["segmento_codigo"])):
            portfolio.append({
                "codigo": p["segmento_codigo"],
                "key": p["segmento"],
                "label": p["segmento_label"],
                "competencia": p["competencia"],
                "indicadores": {
                    "taxa_administracao_pct": p["taxa_administracao_pct"],
                    "grupos_ativos": p["grupos_ativos"],
                    "cotas_ativas_em_dia": p["cotas_ativas_em_dia"],
                    "contemplacoes_mes": p["contemplacoes_mes"],
                    "inadimplencia_participacao": p["inadimplencia_participacao"],
                    "prazo_medio_grupos_meses": p["detalhe_grupos"]["prazo_medio_grupos_meses"],
                    "valor_medio_bem_grupos": p["detalhe_grupos"]["valor_medio_bem_grupos"],
                },
            })

        if not ops:
            coverage = "catalogo"
        elif rep is None:
            coverage = "operacao_sem_registro_reclamacoes"
        elif rep["indice_bc"] is None:
            coverage = "operacao_e_reclamacoes_indice_nao_divulgado"
        else:
            coverage = "operacao_e_indice_reclamacoes_divulgado"

        evidence_signals = ["cadastro_atual_bcb"]
        if ops:
            evidence_signals.append("operacao_mensal_observada")
        if rep is not None:
            evidence_signals.append("registro_reclamacoes_vinculado")
        if rep is not None and rep.get("indice_bc") is not None:
            evidence_signals.append("indice_reclamacoes_bcb_divulgado")

        admins.append({
            "id": root,
            "cnpj_root": root,
            "nome": inst["nome"],
            "identidade": {
                "cadastro_atual_bcb": True,
                "status_detalhado": inst.get("status_detalhado"),
                "nota": "A raiz de oito dígitos identifica a administradora no conjunto cadastral usado; não é CNPJ completo.",
            },
            "portfolio_observado": {
                "segmentos": portfolio,
                "afirmacao_permitida": "Há operação observada nestes segmentos na competência indicada.",
                "limite": "O ConsorcioBD não comprova, sozinho, todos os planos comerciais atualmente disponíveis para contratação.",
            },
            "operacao": {
                "competencia": ops[0]["competencia"] if ops else None,
                "segmentos_count": len(ops),
                "grupos_ativos": availability(total_groups, reason_if_missing="sem_operacao_mensal_vinculada_ou_cobertura_incompleta"),
                "cotas_ativas_em_dia": availability(total_n, reason_if_missing="sem_operacao_mensal_vinculada_ou_cobertura_incompleta"),
                "contemplacoes_mes": availability(total_contempl, reason_if_missing="sem_operacao_mensal_vinculada_ou_cobertura_incompleta"),
                "inadimplentes": availability(total_inad, reason_if_missing="numerador_incompleto_ou_sem_operacao"),
                "cotas_ativas_total_calculado": availability(total_active, reason_if_missing="componentes_incompletos"),
                "inadimplencia_participacao": availability(round(delinquency_share, 8) if delinquency_share is not None else None, reason_if_missing="componentes_incompletos"),
                "nota_inadimplencia": "Participação calculada como inadimplentes/(cotas em dia + inadimplentes), quando ambos os estoques estão disponíveis.",
            },
            "reclamacoes_bcb": rep if rep is not None else {
                "indice_bc": None,
                "indice_status": "sem_registro_vinculado_no_arquivo",
                "posicao_oficial": None,
                "reclamacoes_reguladas_procedentes": None,
                "reclamacoes_reguladas_outras": None,
                "reclamacoes_nao_reguladas": None,
                "reclamacoes_total": None,
                "consorciados_referencia": None,
                "ano": None,
                "semestre": None,
            },
            "presenca": {
                **branch,
                "papel_metodologico": "informativo",
                "nota": "Quantidade de filiais cadastradas não é usada como prova de confiabilidade nem de disponibilidade comercial nacional.",
            },
            "comparabilidade": {
                "coverage": coverage,
                "ranking_geral_publicavel": False,
                "motivo_sem_ranking_geral": "V2 não redistribui pesos entre evidências ausentes e não converte porte/presença em qualidade.",
                "dimensoes_comparaveis": [
                    *(["operacao"] if ops else []),
                    *(["indice_reclamacoes_bcb"] if rep and rep.get("indice_bc") is not None else []),
                    *(["taxa_por_segmento"] if ops else []),
                ],
            },
            "leitura_confiabilidade": {
                "nivel_evidencia": "amplo_para_triagem" if ops and rep else ("parcial" if ops or rep else "insuficiente"),
                "sinais_disponiveis": evidence_signals,
                "texto": "A V2 organiza sinais públicos para triagem e comparação; não certifica solvência, chance de contemplação ou adequação individual.",
            },
        })

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for product in products:
        if product["cnpj_root"] in registry_roots:
            grouped[product["segmento"]].append(product)

    segments: List[Dict[str, Any]] = []
    comparisons: List[Dict[str, Any]] = []
    for segment_key, rows in sorted(grouped.items(), key=lambda kv: min(int(x["segmento_codigo"]) for x in kv[1])):
        label = rows[0]["segmento_label"]
        rates = [x["taxa_administracao_pct"] for x in rows if x["taxa_administracao_pct"] is not None]
        segments.append({
            "segmento": segment_key,
            "label": label,
            "codigo": rows[0]["segmento_codigo"],
            "competencia": rows[0]["competencia"],
            "administradoras_com_operacao_observada": len(rows),
            "cotas_ativas_em_dia": sum(x["cotas_ativas_em_dia"] or 0 for x in rows),
            "contemplacoes_mes": sum(x["contemplacoes_mes"] or 0 for x in rows),
            "taxa_administracao_pct_mediana_administradoras": round(median(rates), 6) if rates else None,
            "nota_taxa": "Mediana derivada entre administradoras observadas; não é média oficial de mercado nem oferta comercial.",
        })
        comparison_rows = []
        for x in rows:
            rep = rankings_by_root.get(x["cnpj_root"], {})
            comparison_rows.append({
                "cnpj_root": x["cnpj_root"],
                "nome": names.get(x["cnpj_root"], x.get("nome_administradora")),
                "taxa_administracao_pct": x["taxa_administracao_pct"],
                "grupos_ativos": x["grupos_ativos"],
                "cotas_ativas_em_dia": x["cotas_ativas_em_dia"],
                "contemplacoes_mes": x["contemplacoes_mes"],
                "inadimplencia_participacao": x["inadimplencia_participacao"],
                "indice_reclamacoes_bcb": rep.get("indice_bc"),
                "indice_reclamacoes_status": rep.get("indice_status", "sem_registro_vinculado_no_arquivo"),
            })
        comparison_rows.sort(key=lambda x: (
            x["taxa_administracao_pct"] is None,
            x["taxa_administracao_pct"] if x["taxa_administracao_pct"] is not None else float("inf"),
            x["nome"] or "",
        ))
        comparisons.append({
            "segmento": segment_key,
            "label": label,
            "competencia": rows[0]["competencia"],
            "administradoras": comparison_rows,
            "sorting": "taxa_administracao_pct crescente apenas para navegação; não representa ranking geral de qualidade.",
            "dimensoes": {
                "taxa_administracao_pct": {"direction": "menor_e_menor_taxa_observada", "meaning": "taxa consolidada observada; não é oferta"},
                "indice_reclamacoes_bcb": {"direction": "menor_e_menor_indice", "meaning": "índice oficial quando divulgado"},
                "inadimplencia_participacao": {"direction": "menor_e_menor_participacao", "meaning": "cálculo derivado dos estoques publicados"},
            },
        })

    return {
        "instituicoes": {"metadata": {"generated_at": generated_at, "contract": CONTRACTS["instituicoes"], "count": len(institutions)}, "items": institutions},
        "administradoras": {"metadata": {"generated_at": generated_at, "contract": CONTRACTS["administradoras"], "count": len(admins), "ranking_geral": False}, "items": admins},
        "produtos": {"metadata": {"generated_at": generated_at, "contract": CONTRACTS["produtos"], "count": len(products), "meaning": "segmentos com operação observada no ConsorcioBD"}, "items": products},
        "rankings": {"metadata": {"generated_at": generated_at, "contract": CONTRACTS["rankings"], "count": len(rankings), "official_position_policy": "null quando a fonte não traz coluna explícita de posição"}, "items": rankings},
        "segmentos": {"metadata": {"generated_at": generated_at, "contract": CONTRACTS["segmentos"], "count": len(segments)}, "items": segments},
        "comparacoes": {"metadata": {"generated_at": generated_at, "contract": CONTRACTS["comparacoes"], "count": len(comparisons), "ranking_geral": False}, "items": comparisons},
    }


def validate_models(models: Dict[str, Any], monthly_meta: Dict[str, Any]) -> None:
    admins = models["administradoras"]["items"]
    products = models["produtos"]["items"]
    if len(admins) < 100:
        raise ValueError("Saída administradoras abaixo do mínimo")
    roots = [x["cnpj_root"] for x in admins]
    if len(roots) != len(set(roots)):
        raise ValueError("Saída administradoras com raiz duplicada")
    if any(x["comparabilidade"].get("ranking_geral_publicavel") for x in admins):
        raise ValueError("Contrato V2 não permite ranking geral nesta versão")
    if monthly_meta.get("competencia") is None or not products:
        raise ValueError("Saída mensal sem competência/produtos")
    if monthly_meta.get("operational_rows_latest") != len(products):
        raise ValueError("Produtos V2 deve conter somente linhas com operação observada")
    for product in products:
        if not has_operational_signal(product):
            raise ValueError("Produto sem sinal operacional foi publicado")
        if "segmentos_consolidados" not in normalize_text(product.get("source_member")):
            raise ValueError("Métrica operacional fora do consolidado canônico")
