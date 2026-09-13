#!/usr/bin/env python3
"""Comparador de Consórcios V2 — read models auditáveis e sem score geral implícito."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Reaproveita apenas utilitários neutros do builder legado. Nenhuma função de score legado é usada.
from build_read_models import (
    dump_json_text,
    load_json,
    normalize_key,
    normalize_text,
    parse_csv_bytes,
    pick_value,
    try_load_json,
    write_json_if_changed,
)

PIPELINE_VERSION = "4.0.0"
CONTRACTS = {
    "administradoras": "administradoras.v2",
    "instituicoes": "instituicoes.v2",
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


def root8(value: Any) -> Optional[str]:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) == 8:
        return digits
    if len(digits) == 14:
        return digits[:8]
    return None


def strict_number(value: Any) -> Optional[float]:
    """Converte apenas representação numérica inteira/decimal; não extrai número de texto arbitrário."""
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
    return {normalize_key(str(k)): v for k, v in record.items()}


def load_registry(path: Path) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw in read_json_records(path):
        row = normalized_record(raw)
        name = pick_value(row, ["institution_name", "nome", "name", "noenti", "razao_social", "nome_fantasia"])
        root = root8(pick_value(row, ["cnpj", "cpf_cnpj", "cnpj14", "coenti", "institution_id"]))
        if not name or not root:
            continue
        if root in seen:
            raise ValueError(f"Cadastro com raiz duplicada: {root}")
        seen.add(root)
        items.append({
            "cnpj_root": root,
            "nome": str(name).strip(),
            "nome_normalizado": normalize_text(str(name)),
            "status_detalhado": pick_value(row, ["status", "situacao", "situacao_cadastral"]),
        })
    if len(items) < 100:
        raise ValueError(f"Cadastro crítico abaixo do mínimo: {len(items)}")
    return items


def load_branches(path: Path) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    by_root: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"filiais": 0, "ufs": set(), "municipios": set()})
    unresolved: List[Dict[str, Any]] = []
    for raw in read_json_records(path):
        row = normalized_record(raw)
        root = root8(pick_value(row, ["cnpj", "cpf_cnpj", "cnpj14", "cnpj_da_administradora", "coenti"]))
        if not root:
            unresolved.append({"nome": pick_value(row, ["institution_name", "nome", "administradora", "noenti"])})
            continue
        item = by_root[root]
        item["filiais"] += 1
        uf = pick_value(row, ["uf", "sigla_uf"])
        city = pick_value(row, ["cidade", "municipio", "municipio_nome"])
        if uf:
            item["ufs"].add(str(uf).strip())
        if city:
            item["municipios"].add(str(city).strip())
    clean = {
        root: {"filiais": v["filiais"], "ufs": sorted(v["ufs"]), "municipios_count": len(v["municipios"])}
        for root, v in by_root.items()
    }
    return clean, unresolved


def load_rankings_v2(path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    rows, csv_meta = parse_csv_bytes(path.read_bytes())
    parsed: List[Dict[str, Any]] = []
    for row in rows:
        name = pick_value(row, ["administradora_de_consorcio", "nome_da_administradora", "instituicao", "nome", "administradora"])
        root = root8(pick_value(row, ["cnpj_ac", "cnpj", "cnpj_da_administradora"]))
        if not name or not root:
            continue
        index_value = strict_number(pick_value(row, ["indice", "indice_reclamacoes", "indice_de_reclamacoes"]))
        official_position = strict_int(pick_value(row, ["posicao", "ranking", "classificacao"]))
        parsed.append({
            "cnpj_root": root,
            "nome": str(name).strip(),
            "nome_normalizado": normalize_text(str(name)),
            "indice_bc": index_value,
            "indice_status": "divulgado" if index_value is not None else "nao_divulgado_pela_fonte",
            "posicao_oficial": official_position,
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


def load_monthly_v2(path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    consolidated: List[Tuple[str, Dict[str, Any]]] = []
    group_rows: List[Tuple[str, Dict[str, Any]]] = []
    with zipfile.ZipFile(path, "r") as zf:
        for member in zf.namelist():
            if not member.lower().endswith(".csv"):
                continue
            key = normalize_text(member)
            if "significado" in key or "layout" in key:
                continue
            rows, _ = parse_csv_bytes(zf.read(member))
            if "segmentos_consolidados" in key:
                consolidated.extend((member, r) for r in rows)
            elif "grupos" in key:
                group_rows.extend((member, r) for r in rows)
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
    selected = [r for (_root, period, _segment), r in records.items() if period == latest]
    if len({x["cnpj_root"] for x in selected}) < 100:
        raise ValueError("Cobertura mensal crítica abaixo do mínimo de 100 administradoras")

    group_acc: Dict[Tuple[str, str, str], Dict[str, float]] = defaultdict(lambda: {
        "prazo_total": 0.0, "prazo_weight": 0.0, "credito_total": 0.0, "credito_weight": 0.0, "rows": 0.0
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
    for r in selected:
        acc = group_acc.get((r["cnpj_root"], latest, r["segmento_codigo"]))
        r["detalhe_grupos"] = {
            "disponivel": bool(acc),
            "grupos_linhas": int(acc["rows"]) if acc else 0,
            "prazo_medio_grupos_meses": round(acc["prazo_total"] / acc["prazo_weight"], 4) if acc and acc["prazo_weight"] > 0 else None,
            "valor_medio_bem_grupos": round(acc["credito_total"] / acc["credito_weight"], 2) if acc and acc["credito_weight"] > 0 else None,
            "nota": "Detalhes por grupo são usados apenas para prazo/crédito; estoques, fluxos e taxa vêm do consolidado.",
        }
    selected.sort(key=lambda x: (x["cnpj_root"], x["segmento_codigo"]))
    return selected, {
        "competencia": latest,
        "periodos_encontrados": sorted(periods),
        "records": len(selected),
        "administradoras": len({x["cnpj_root"] for x in selected}),
        "segmentos": sorted({x["segmento_codigo"] for x in selected}),
    }


def source_fingerprint(paths: Iterable[Path], version: str) -> str:
    h = hashlib.sha256(version.encode())
    for path in paths:
        h.update(str(path).encode())
        h.update(path.read_bytes())
    return h.hexdigest()


def stable_generated_at(meta_path: Path, fingerprint: str) -> str:
    previous = try_load_json(meta_path)
    if isinstance(previous, dict) and previous.get("source_fingerprint") == fingerprint and previous.get("generated_at"):
        return str(previous["generated_at"])
    return utc_now_iso()


def build_models(registry: List[Dict[str, Any]], branches: Dict[str, Dict[str, Any]], rankings: List[Dict[str, Any]], products: List[Dict[str, Any]], generated_at: str) -> Dict[str, Any]:
    registry_roots = {x["cnpj_root"] for x in registry}
    products_by_root: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for product in products:
        products_by_root[product["cnpj_root"]].append(product)
    rankings_by_root = {x["cnpj_root"]: x for x in rankings}
    names = {x["cnpj_root"]: x["nome"] for x in registry}

    institutions = []
    admins = []
    for inst in sorted(registry, key=lambda x: x["nome"]):
        root = inst["cnpj_root"]
        ops = products_by_root.get(root, [])
        rep = rankings_by_root.get(root)
        branch = branches.get(root, {"filiais": 0, "ufs": [], "municipios_count": 0})
        institutions.append({**inst, "cadastro_atual_bcb": True, "presenca_cadastrada": branch})

        portfolio = [{
            "codigo": p["segmento_codigo"], "key": p["segmento"], "label": p["segmento_label"], "competencia": p["competencia"]
        } for p in ops]
        total_n = sum(p["cotas_ativas_em_dia"] for p in ops if p["cotas_ativas_em_dia"] is not None) if ops else None
        inad_values = [p["inadimplentes"] for p in ops]
        total_inad = sum(x for x in inad_values if x is not None) if ops and all(x is not None for x in inad_values) else None
        total_active = total_n + total_inad if total_n is not None and total_inad is not None else None
        delinquency_share = total_inad / total_active if total_active and total_inad is not None else None

        if not ops:
            coverage = "catalogo"
        elif rep is None:
            coverage = "operacao_sem_registro_reclamacoes"
        elif rep["indice_bc"] is None:
            coverage = "operacao_e_reclamacoes_indice_nao_divulgado"
        else:
            coverage = "operacao_e_indice_reclamacoes_divulgado"

        admins.append({
            "id": root,
            "cnpj_root": root,
            "nome": inst["nome"],
            "identidade": {
                "cadastro_atual_bcb": True,
                "status_detalhado": inst.get("status_detalhado"),
                "nota": "A raiz de oito dígitos identifica a administradora no conjunto cadastral usado; não é CNPJ completo."
            },
            "portfolio_observado": {
                "segmentos": portfolio,
                "afirmacao_permitida": "Há operação observada nestes segmentos na competência indicada.",
                "limite": "O ConsorcioBD não comprova, sozinho, todos os planos/produtos comerciais atualmente disponíveis para contratação."
            },
            "operacao": {
                "competencia": ops[0]["competencia"] if ops else None,
                "segmentos_count": len(ops),
                "cotas_ativas_em_dia": availability(total_n, reason_if_missing="sem_operacao_mensal_vinculada"),
                "inadimplentes": availability(total_inad, reason_if_missing="numerador_incompleto_ou_sem_operacao"),
                "cotas_ativas_total_calculado": availability(total_active, reason_if_missing="componentes_incompletos"),
                "inadimplencia_participacao": availability(round(delinquency_share, 8) if delinquency_share is not None else None, reason_if_missing="componentes_incompletos"),
                "nota_inadimplencia": "Participação calculada como inadimplentes/(cotas em dia + inadimplentes), quando ambos os estoques estão disponíveis."
            },
            "reclamacoes_bcb": rep if rep is not None else {
                "indice_bc": None, "indice_status": "sem_registro_vinculado_no_arquivo", "posicao_oficial": None,
                "reclamacoes_reguladas_procedentes": None, "reclamacoes_reguladas_outras": None,
                "reclamacoes_nao_reguladas": None, "reclamacoes_total": None, "consorciados_referencia": None,
                "ano": None, "semestre": None
            },
            "presenca": {
                **branch,
                "papel_metodologico": "informativo",
                "nota": "Quantidade de filiais cadastradas não é usada como prova de confiabilidade nem de disponibilidade comercial nacional."
            },
            "comparabilidade": {
                "coverage": coverage,
                "ranking_geral_publicavel": False,
                "motivo_sem_ranking_geral": "V2 não redistribui pesos entre evidências ausentes e não converte porte/presença em qualidade.",
                "dimensoes_comparaveis": [
                    *(["operacao"] if ops else []),
                    *(["indice_reclamacoes_bcb"] if rep and rep.get("indice_bc") is not None else []),
                    *(["taxa_por_segmento"] if ops else [])
                ]
            },
            "leitura_confiabilidade": {
                "estado": "evidencia_suficiente_para_triagem" if ops and rep else ("evidencia_parcial" if ops or rep else "dados_insuficientes"),
                "texto": "A V2 organiza sinais públicos para triagem e comparação; não certifica solvência, chance de contemplação ou adequação individual."
            }
        })

    segment_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for product in products:
        if product["cnpj_root"] in registry_roots:
            segment_groups[product["segmento"]].append(product)
    segments = []
    comparisons = []
    for key, rows in sorted(segment_groups.items(), key=lambda kv: min(int(x["segmento_codigo"]) for x in kv[1])):
        label = rows[0]["segmento_label"]
        rates = [x["taxa_administracao_pct"] for x in rows if x["taxa_administracao_pct"] is not None]
        segments.append({
            "segmento": key,
            "label": label,
            "codigo": rows[0]["segmento_codigo"],
            "competencia": rows[0]["competencia"],
            "administradoras_com_operacao": len(rows),
            "cotas_ativas_em_dia": sum(x["cotas_ativas_em_dia"] or 0 for x in rows),
            "contemplacoes_mes": sum(x["contemplacoes_mes"] or 0 for x in rows),
            "taxa_administracao_pct_mediana_administradoras": round(median(rates), 6) if rates else None,
            "nota_taxa": "Mediana derivada entre administradoras observadas; não é média oficial de mercado nem oferta comercial."
        })
        comparisons.append({
            "segmento": key,
            "label": label,
            "competencia": rows[0]["competencia"],
            "administradoras": [{
                "cnpj_root": x["cnpj_root"],
                "nome": names.get(x["cnpj_root"], x.get("nome_administradora")),
                "taxa_administracao_pct": x["taxa_administracao_pct"],
                "grupos_ativos": x["grupos_ativos"],
                "cotas_ativas_em_dia": x["cotas_ativas_em_dia"],
                "contemplacoes_mes": x["contemplacoes_mes"],
                "inadimplencia_participacao": x["inadimplencia_participacao"],
                "indice_reclamacoes_bcb": rankings_by_root.get(x["cnpj_root"], {}).get("indice_bc"),
                "indice_reclamacoes_status": rankings_by_root.get(x["cnpj_root"], {}).get("indice_status", "sem_registro_vinculado_no_arquivo")
            } for x in sorted(rows, key=lambda x: (x["taxa_administracao_pct"] is None, x["taxa_administracao_pct"] or 0, x["cnpj_root"]))],
            "sorting": "taxa_administracao_pct crescente apenas para navegação; não representa ranking geral de qualidade."
        })

    return {
        "instituicoes": {"metadata": {"generated_at": generated_at, "contract": CONTRACTS["instituicoes"], "count": len(institutions)}, "items": institutions},
        "administradoras": {"metadata": {"generated_at": generated_at, "contract": CONTRACTS["administradoras"], "count": len(admins), "ranking_geral": False}, "items": admins},
        "produtos": {"metadata": {"generated_at": generated_at, "contract": CONTRACTS["produtos"], "count": len(products), "meaning": "segmentos com operação observada no ConsorcioBD"}, "items": products},
        "rankings": {"metadata": {"generated_at": generated_at, "contract": CONTRACTS["rankings"], "count": len(rankings), "official_position_policy": "null quando a fonte não traz coluna de posição"}, "items": rankings},
        "segmentos": {"metadata": {"generated_at": generated_at, "contract": CONTRACTS["segmentos"], "count": len(segments)}, "items": segments},
        "comparacoes": {"metadata": {"generated_at": generated_at, "contract": CONTRACTS["comparacoes"], "count": len(comparisons), "ranking_geral": False}, "items": comparisons}
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
    for product in products:
        if "segmentos_consolidados" not in normalize_text(product.get("source_member") or ""):
            raise ValueError("Métrica operacional fora do consolidado canônico")


def build_seo_contracts(generated_at: str, admins_count: int, routes_config: Any) -> Dict[str, Any]:
    return {
        "defaults": {
            "contract": "seo.defaults.v2",
            "generated_at": generated_at,
            "comparison_policy": "sem ranking geral; evidências e comparações por dimensão",
            "administradoras_count": admins_count
        },
        "routes": {
            "contract": "seo.routes.v2",
            "generated_at": generated_at,
            "source_config": routes_config
        },
        "site": {
            "contract": "seo.site.v2",
            "generated_at": generated_at,
            "site": "Sanida",
            "path": "/financas/consorcio/",
            "purpose": "identidade, operação observada, sinais públicos e comparação de administradoras de consórcio"
        }
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--seo-routes", required=True)
    args = parser.parse_args()

    cfg = load_json(Path(args.config))
    defaults = cfg.get("defaults", {})
    raw_base = Path(defaults.get("raw_base_dir", "data/raw"))
    stage_base = Path(defaults.get("stage_base_dir", "data/stage"))
    dist_base = Path(defaults.get("dist_base_dir", "data/dist"))
    global_dir = dist_base / "global"
    seo_dir = dist_base / "seo"
    runtime_dir = Path("data/runtime")
    global_dir.mkdir(parents=True, exist_ok=True)
    seo_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    cadastro = stage_base / "cadastro" / "instituicoes_cadastro.json"
    filiais = stage_base / "filiais" / "filiais.json"
    monthly = raw_base / "bc" / "consorciobd" / "latest_source.bin"
    ranking = raw_base / "bc" / "ranking_reclamacoes" / "latest_source.csv"
    required = [cadastro, filiais, monthly, ranking]
    missing = [str(x) for x in required if not x.exists()]
    if missing:
        raise SystemExit("Insumos V2 obrigatórios ausentes: " + ", ".join(missing))

    fingerprint = source_fingerprint(required + [Path(args.seo_routes)], PIPELINE_VERSION)
    generated_at = stable_generated_at(global_dir / "meta.json", fingerprint)

    registry = load_registry(cadastro)
    branch_map, branch_unresolved = load_branches(filiais)
    rankings, ranking_meta = load_rankings_v2(ranking)
    products, monthly_meta = load_monthly_v2(monthly)
    models = build_models(registry, branch_map, rankings, products, generated_at)
    validate_models(models, monthly_meta)

    models["ofertas"] = {
        "metadata": {"generated_at": generated_at, "contract": CONTRACTS["ofertas"], "count": 0},
        "items": [],
        "note": "Ofertas comerciais permanecem separadas das evidências institucionais e não alteram comparação."
    }

    routes_config = load_json(Path(args.seo_routes))
    seo_payloads = build_seo_contracts(generated_at, len(models["administradoras"]["items"]), routes_config)

    artifact_entries = {"global": [], "seo": []}
    for name, payload in models.items():
        filename = f"{name}.json"
        text = dump_json_text(payload)
        artifact_entries["global"].append({"file": filename, "sha256": hashlib.sha256(text.encode()).hexdigest(), "size_bytes": len(text.encode())})
    for name, payload in seo_payloads.items():
        filename = f"{name}.json"
        text = dump_json_text(payload)
        artifact_entries["seo"].append({"file": filename, "sha256": hashlib.sha256(text.encode()).hexdigest(), "size_bytes": len(text.encode())})

    meta = {
        "generated_at": generated_at,
        "pipeline_version": PIPELINE_VERSION,
        "source_fingerprint": fingerprint,
        "contracts": CONTRACTS,
        "methodology": {
            "purpose": "triagem e comparação por sinais públicos",
            "general_score": False,
            "missingness_policy": "ausência não recebe zero, nota neutra nem redistribuição de peso",
            "presence_role": "informativo",
            "commercial_offers_affect_assessment": False
        },
        "source_periods": {"consorciobd_mensal": monthly_meta.get("competencia")},
        "counts": {k: len(v.get("items", [])) for k, v in models.items()},
        "quality": {
            "registry_roots": len(registry),
            "monthly_roots": monthly_meta.get("administradoras"),
            "ranking_rows": ranking_meta.get("row_count"),
            "branch_unresolved": len(branch_unresolved),
            "operational_orphans": sorted({p["cnpj_root"] for p in products} - {x["cnpj_root"] for x in registry}),
            "ranking_orphans": sorted({r["cnpj_root"] for r in rankings} - {x["cnpj_root"] for x in registry})
        },
        "artifacts": artifact_entries,
        "notes": [
            "meta.json não contém auto-hash.",
            "Todos os demais JSONs consumíveis produzidos por este builder constam no manifesto com hash dos bytes serializados."
        ]
    }

    changed = False
    for name, payload in models.items():
        changed |= write_json_if_changed(global_dir / f"{name}.json", payload)
    for name, payload in seo_payloads.items():
        changed |= write_json_if_changed(seo_dir / f"{name}.json", payload)
    changed |= write_json_if_changed(global_dir / "meta.json", meta)

    runtime = {
        "last_checked_at": utc_now_iso(),
        "last_changed_at": utc_now_iso() if changed else None,
        "changed": changed,
        "mode_used": "build-read-models-v2",
        "pipeline_version": PIPELINE_VERSION,
        "source_fingerprint": fingerprint,
        "competencia": monthly_meta.get("competencia")
    }
    write_json_if_changed(runtime_dir / "build_read_models_v2.json", runtime)

    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as fh:
            fh.write(f"changed={'true' if changed else 'false'}\n")
            fh.write("mode_used=build-read-models-v2\n")
            fh.write(f"records={len(models['administradoras']['items'])}\n")
    print(json.dumps({
        "changed": changed,
        "mode_used": "build-read-models-v2",
        "records": len(models["administradoras"]["items"]),
        "competencia": monthly_meta.get("competencia")
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
