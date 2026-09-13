#!/usr/bin/env python3
"""Camada de interpretação relativa do Comparador de Consórcios V2.

Esta camada transforma métricas já validadas em contexto comparável e mensagens
restritas ao que os dados permitem afirmar. Ela não cria score geral, não converte
porte em qualidade, não preenche ausências e não infere chance/tempo de contemplação.
"""
from __future__ import annotations

from copy import deepcopy
from math import floor, isclose
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional, Sequence

INTERPRETATION_CONTRACT = "interpretacao-relativa.v1"

SEGMENT_METRICS: Dict[str, Dict[str, str]] = {
    "taxa_administracao_pct": {
        "order": "asc",
        "role": "custo_observado",
        "label": "Taxa de administração observada",
        "scope": "segmento_mesma_competencia",
    },
    "cotas_ativas_em_dia": {
        "order": "desc",
        "role": "escala_operacional",
        "label": "Cotas ativas em dia",
        "scope": "segmento_mesma_competencia",
    },
    "contemplacoes_mes": {
        "order": "desc",
        "role": "volume_absoluto_observado",
        "label": "Contemplações observadas no mês",
        "scope": "segmento_mesma_competencia",
    },
    "inadimplencia_participacao": {
        "order": "asc",
        "role": "participacao_inadimplencia_derivada",
        "label": "Participação calculada de inadimplência",
        "scope": "segmento_mesma_competencia",
    },
}

COMPLAINTS_METRIC = {
    "order": "asc",
    "role": "indice_reclamacoes_institucional",
    "label": "Índice de reclamações BCB",
    "scope": "administradoras_cadastro_atual_com_indice_divulgado",
}


def _as_number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _round(value: Optional[float], digits: int = 8) -> Optional[float]:
    if value is None:
        return None
    rounded = round(float(value), digits)
    return 0.0 if rounded == -0.0 else rounded


def _quantile(sorted_values: Sequence[float], p: float) -> Optional[float]:
    """Quantil por interpolação linear sobre posição p*(n-1)."""
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    pos = (len(sorted_values) - 1) * p
    lower = floor(pos)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = pos - lower
    return float(sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight)


def distribution(values: Iterable[Any]) -> Dict[str, Any]:
    numeric = sorted(v for v in (_as_number(x) for x in values) if v is not None)
    if not numeric:
        return {
            "n": 0,
            "min": None,
            "q1": None,
            "mediana": None,
            "q3": None,
            "max": None,
            "metodo_quartis": "interpolacao_linear_p_n_menos_1",
        }
    return {
        "n": len(numeric),
        "min": _round(numeric[0]),
        "q1": _round(_quantile(numeric, 0.25)),
        "mediana": _round(_quantile(numeric, 0.50)),
        "q3": _round(_quantile(numeric, 0.75)),
        "max": _round(numeric[-1]),
        "metodo_quartis": "interpolacao_linear_p_n_menos_1",
    }


def dense_rank(values_by_id: Mapping[str, Any], *, order: str) -> Dict[str, Dict[str, int]]:
    valid: Dict[str, float] = {}
    for item_id, raw in values_by_id.items():
        value = _as_number(raw)
        if value is not None:
            valid[str(item_id)] = value
    reverse = order == "desc"
    unique = sorted(set(valid.values()), reverse=reverse)
    rank_by_value = {value: idx + 1 for idx, value in enumerate(unique)}
    universe = len(valid)
    return {
        item_id: {"posicao": rank_by_value[value], "universo": universe}
        for item_id, value in valid.items()
    }


def _median_relation(value: float, median_value: Optional[float]) -> Optional[str]:
    if median_value is None:
        return None
    if isclose(value, median_value, rel_tol=1e-12, abs_tol=1e-12):
        return "igual_mediana"
    return "abaixo_mediana" if value < median_value else "acima_mediana"


def _distribution_band(value: float, stats: Mapping[str, Any]) -> Optional[str]:
    if int(stats.get("n") or 0) < 4:
        return "amostra_insuficiente_para_quartis"
    q1 = _as_number(stats.get("q1"))
    med = _as_number(stats.get("mediana"))
    q3 = _as_number(stats.get("q3"))
    if q1 is None or med is None or q3 is None:
        return None
    if value <= q1:
        return "quartil_inferior"
    if value >= q3:
        return "quartil_superior"
    if isclose(value, med, rel_tol=1e-12, abs_tol=1e-12):
        return "mediana"
    if value < med:
        return "entre_q1_e_mediana"
    return "entre_mediana_e_q3"


def _highlight_text(metric: str, band: Optional[str]) -> Optional[str]:
    if band not in {"quartil_inferior", "quartil_superior"}:
        return None
    low = band == "quartil_inferior"
    if metric == "taxa_administracao_pct":
        return (
            "A taxa observada está entre o quarto de menores taxas do segmento nesta competência."
            if low
            else "A taxa observada está entre o quarto de maiores taxas do segmento nesta competência."
        )
    if metric == "cotas_ativas_em_dia":
        return (
            "A operação observada por cotas em dia está entre o quarto de menores do segmento; porte não é qualidade."
            if low
            else "A operação observada por cotas em dia está entre o quarto de maiores do segmento; porte não é qualidade."
        )
    if metric == "contemplacoes_mes":
        return (
            "O volume absoluto de contemplações no mês está entre o quarto de menores do segmento; isso não mede chance nem velocidade individual."
            if low
            else "O volume absoluto de contemplações no mês está entre o quarto de maiores do segmento; isso não mede chance nem velocidade individual."
        )
    if metric == "inadimplencia_participacao":
        return (
            "A participação calculada de inadimplência está entre o quarto de menores do segmento nesta competência."
            if low
            else "A participação calculada de inadimplência está entre o quarto de maiores do segmento nesta competência."
        )
    if metric == "indice_reclamacoes_bcb":
        return (
            "O índice de reclamações divulgado está entre o quarto de menores índices do universo comparável atual."
            if low
            else "O índice de reclamações divulgado está entre o quarto de maiores índices do universo comparável atual."
        )
    return None


def metric_context(
    *,
    metric: str,
    value: Any,
    stats: Mapping[str, Any],
    rank: Optional[Mapping[str, int]],
    order: str,
    role: str,
    scope: str,
) -> Dict[str, Any]:
    number = _as_number(value)
    if number is None:
        return {
            "availability": "unavailable",
            "reason": "valor_ausente",
            "mediana_referencia": stats.get("mediana"),
            "ordem_derivada": None,
            "comparacao_mediana": None,
            "faixa_distribuicao": None,
            "destaque": None,
            "papel": role,
            "escopo": scope,
        }
    median_value = _as_number(stats.get("mediana"))
    delta = number - median_value if median_value is not None else None
    delta_pct = None
    if median_value not in (None, 0.0):
        delta_pct = delta / median_value
    band = _distribution_band(number, stats)
    derived_rank = None
    if rank:
        derived_rank = {
            "posicao": int(rank["posicao"]),
            "universo": int(rank["universo"]),
            "sentido": "menor_primeiro" if order == "asc" else "maior_primeiro",
            "oficial": False,
            "nota": "Ordem derivada apenas deste critério; não é ranking geral nem posição oficial do Banco Central.",
        }
    return {
        "availability": "available",
        "reason": None,
        "mediana_referencia": _round(median_value),
        "comparacao_mediana": _median_relation(number, median_value),
        "diferenca_mediana": _round(delta),
        "diferenca_mediana_relativa": _round(delta_pct),
        "faixa_distribuicao": band,
        "ordem_derivada": derived_rank,
        "destaque": _highlight_text(metric, band),
        "papel": role,
        "escopo": scope,
    }


def _complaints_context(admin: MutableMapping[str, Any], stats: Mapping[str, Any], ranks: Mapping[str, Mapping[str, int]]) -> Dict[str, Any]:
    rep = admin.get("reclamacoes_bcb") or {}
    value = rep.get("indice_bc") if isinstance(rep, dict) else None
    root = str(admin.get("cnpj_root") or "")
    if _as_number(value) is None:
        status = rep.get("indice_status") if isinstance(rep, dict) else None
        return {
            "availability": "unavailable",
            "reason": status or "indice_nao_disponivel",
            "mediana_referencia": stats.get("mediana"),
            "ordem_derivada": None,
            "comparacao_mediana": None,
            "faixa_distribuicao": None,
            "destaque": None,
            "papel": COMPLAINTS_METRIC["role"],
            "escopo": COMPLAINTS_METRIC["scope"],
            "nota": "Índice ausente ou não divulgado não equivale a zero nem a sinal favorável.",
        }
    context = metric_context(
        metric="indice_reclamacoes_bcb",
        value=value,
        stats=stats,
        rank=ranks.get(root),
        order=COMPLAINTS_METRIC["order"],
        role=COMPLAINTS_METRIC["role"],
        scope=COMPLAINTS_METRIC["scope"],
    )
    context["nota"] = "O índice é institucional e não específico de um segmento de consórcio."
    return context


def _trust_reading(admin: MutableMapping[str, Any]) -> Dict[str, Any]:
    portfolio = admin.get("portfolio_observado") or {}
    segments = portfolio.get("segmentos") if isinstance(portfolio, dict) else []
    has_ops = bool(segments)
    rep = admin.get("reclamacoes_bcb") or {}
    rep_status = rep.get("indice_status") if isinstance(rep, dict) else None
    has_rep = rep_status not in (None, "sem_registro_vinculado_no_arquivo")
    has_index = isinstance(rep, dict) and _as_number(rep.get("indice_bc")) is not None

    if has_ops and has_index:
        key = "evidencia_ampla_para_triagem"
        text = (
            "Há cadastro atual no Banco Central, operação mensal observada e índice de reclamações divulgado. "
            "Isso sustenta uma triagem mais ampla, mas não certifica confiabilidade nem adequação de uma proposta."
        )
    elif has_ops and has_rep:
        key = "evidencia_ampla_com_indice_nao_divulgado"
        text = (
            "Há cadastro atual, operação mensal observada e registro de reclamações vinculado, mas o índice não foi divulgado pela fonte. "
            "A ausência do índice não equivale a zero nem a ausência de reclamações."
        )
    elif has_ops:
        key = "evidencia_operacional_sem_registro_reclamacoes_vinculado"
        text = (
            "Há cadastro atual e operação mensal observada, mas não há registro de reclamações vinculado no arquivo consumido. "
            "Isso limita a leitura de reclamações e não autoriza concluir que a administradora não tenha reclamações."
        )
    elif has_rep:
        key = "evidencia_reclamacoes_sem_operacao_mensal_observada"
        text = (
            "Há cadastro atual e registro de reclamações vinculado, mas não há operação mensal observada nos segmentos publicados. "
            "A evidência é parcial e não sustenta comparação operacional por segmento."
        )
    else:
        key = "apenas_cadastro_atual"
        text = (
            "A administradora aparece no cadastro atual usado pela ferramenta, mas as demais evidências desta release são insuficientes para uma triagem comparativa ampla."
        )

    return {
        "contract": INTERPRETATION_CONTRACT,
        "resposta_chave": key,
        "texto": text,
        "sinais": {
            "cadastro_atual_bcb": bool((admin.get("identidade") or {}).get("cadastro_atual_bcb")),
            "operacao_mensal_observada": has_ops,
            "registro_reclamacoes_vinculado": has_rep,
            "indice_reclamacoes_bcb_divulgado": has_index,
        },
        "limites": [
            "Não certifica solvência nem confiabilidade universal.",
            "Não substitui análise da proposta, do regulamento e das condições do grupo.",
            "Não prevê chance nem tempo individual de contemplação.",
        ],
    }


def _interpretation_config(methodology: Mapping[str, Any]) -> Mapping[str, Any]:
    cfg = methodology.get("relative_interpretation")
    if not isinstance(cfg, dict) or cfg.get("enabled") is not True:
        raise ValueError("methodology_v2 deve habilitar relative_interpretation")
    if cfg.get("contract") != INTERPRETATION_CONTRACT:
        raise ValueError("Contrato de interpretação incompatível com o código")
    if cfg.get("general_ranking") is not False:
        raise ValueError("Interpretação V2 não permite ranking geral")
    return cfg


def enrich_models(models: Mapping[str, Any], methodology: Mapping[str, Any]) -> Dict[str, Any]:
    """Retorna cópia enriquecida dos modelos V2 com interpretação relativa auditável."""
    _interpretation_config(methodology)
    out = deepcopy(dict(models))

    admins = out["administradoras"]["items"]
    segments = out["segmentos"]["items"]
    comparisons = out["comparacoes"]["items"]

    complaints_values = {
        str(admin["cnpj_root"]): admin.get("reclamacoes_bcb", {}).get("indice_bc")
        for admin in admins
    }
    complaints_stats = distribution(complaints_values.values())
    complaints_ranks = dense_rank(complaints_values, order="asc")

    admin_by_root: Dict[str, MutableMapping[str, Any]] = {
        str(admin["cnpj_root"]): admin for admin in admins
    }
    segment_by_key: Dict[str, MutableMapping[str, Any]] = {
        str(segment["segmento"]): segment for segment in segments
    }

    for admin in admins:
        trust = _trust_reading(admin)
        admin["leitura_confiabilidade"] = {
            **(admin.get("leitura_confiabilidade") or {}),
            **trust,
        }
        admin["interpretacao_relativa"] = {
            "contract": INTERPRETATION_CONTRACT,
            "reclamacoes_bcb": _complaints_context(admin, complaints_stats, complaints_ranks),
        }

    for comparison in comparisons:
        segment_key = str(comparison["segmento"])
        rows = comparison.get("administradoras") or []
        stats_by_metric: Dict[str, Dict[str, Any]] = {}
        ranks_by_metric: Dict[str, Dict[str, Dict[str, int]]] = {}

        for metric, definition in SEGMENT_METRICS.items():
            values_by_root = {
                str(row["cnpj_root"]): row.get(metric)
                for row in rows
            }
            stats_by_metric[metric] = distribution(values_by_root.values())
            ranks_by_metric[metric] = dense_rank(values_by_root, order=definition["order"])

        comparison["interpretacao"] = {
            "contract": INTERPRETATION_CONTRACT,
            "general_ranking": False,
            "criterios": {
                metric: {
                    **definition,
                    "referencia": stats_by_metric[metric],
                    "ordem_derivada_permitida": True,
                    "nota": "A ordem pertence somente ao critério indicado e não cria uma classificação geral de qualidade.",
                }
                for metric, definition in SEGMENT_METRICS.items()
            },
            "contemplacao": {
                "conclusao": "nao_inferivel",
                "texto": "Contemplações observadas no mês são volume absoluto; não permitem concluir qual administradora contempla mais rápido nem estimar a chance individual de contemplação.",
            },
        }

        segment = segment_by_key.get(segment_key)
        if segment is not None:
            segment["interpretacao"] = {
                "contract": INTERPRETATION_CONTRACT,
                "referencias": {
                    metric: {
                        **SEGMENT_METRICS[metric],
                        "distribuicao": stats_by_metric[metric],
                    }
                    for metric in SEGMENT_METRICS
                },
                "contemplacao": comparison["interpretacao"]["contemplacao"],
            }

        for row in rows:
            root = str(row["cnpj_root"])
            contexts = {}
            for metric, definition in SEGMENT_METRICS.items():
                contexts[metric] = metric_context(
                    metric=metric,
                    value=row.get(metric),
                    stats=stats_by_metric[metric],
                    rank=ranks_by_metric[metric].get(root),
                    order=definition["order"],
                    role=definition["role"],
                    scope=definition["scope"],
                )
            admin = admin_by_root.get(root)
            complaint_context = (
                admin.get("interpretacao_relativa", {}).get("reclamacoes_bcb")
                if admin is not None
                else None
            )
            row["interpretacao_relativa"] = {
                "contract": INTERPRETATION_CONTRACT,
                "metricas_segmento": contexts,
                "reclamacoes_bcb": deepcopy(complaint_context),
            }

            if admin is not None:
                portfolio = admin.get("portfolio_observado", {}).get("segmentos", [])
                for portfolio_item in portfolio:
                    if portfolio_item.get("key") == segment_key:
                        portfolio_item["interpretacao_relativa"] = {
                            "contract": INTERPRETATION_CONTRACT,
                            "competencia": comparison.get("competencia"),
                            "metricas": deepcopy(contexts),
                            "destaques": [
                                ctx["destaque"]
                                for ctx in contexts.values()
                                if ctx.get("destaque")
                            ],
                        }
                        break

    out["administradoras"]["metadata"]["interpretation_contract"] = INTERPRETATION_CONTRACT
    out["segmentos"]["metadata"]["interpretation_contract"] = INTERPRETATION_CONTRACT
    out["comparacoes"]["metadata"]["interpretation_contract"] = INTERPRETATION_CONTRACT
    out["administradoras"]["metadata"]["reclamacoes_bcb_referencia_global"] = complaints_stats
    return out


def validate_interpretations(models: Mapping[str, Any], methodology: Mapping[str, Any]) -> None:
    _interpretation_config(methodology)
    admins = models["administradoras"]["items"]
    comparisons = models["comparacoes"]["items"]
    segments = models["segmentos"]["items"]

    if not admins or not comparisons or not segments:
        raise ValueError("Interpretação V2 exige administradoras, comparações e segmentos")

    for family in ("administradoras", "segmentos", "comparacoes"):
        if models[family]["metadata"].get("interpretation_contract") != INTERPRETATION_CONTRACT:
            raise ValueError(f"{family} sem contrato de interpretação")

    for admin in admins:
        if admin.get("comparabilidade", {}).get("ranking_geral_publicavel") is not False:
            raise ValueError("Interpretação não pode habilitar ranking geral")
        reading = admin.get("leitura_confiabilidade") or {}
        if reading.get("contract") != INTERPRETATION_CONTRACT:
            raise ValueError("Administradora sem leitura de confiabilidade contratada")
        complaints = admin.get("interpretacao_relativa", {}).get("reclamacoes_bcb") or {}
        if complaints.get("availability") == "unavailable" and complaints.get("ordem_derivada") is not None:
            raise ValueError("Índice ausente não pode receber ordem derivada")

    for comparison in comparisons:
        if comparison.get("interpretacao", {}).get("general_ranking") is not False:
            raise ValueError("Comparação não pode declarar ranking geral")
        if comparison.get("interpretacao", {}).get("contemplacao", {}).get("conclusao") != "nao_inferivel":
            raise ValueError("Política de contemplação deve impedir inferência de velocidade/chance")
        for row in comparison.get("administradoras", []):
            contexts = row.get("interpretacao_relativa", {}).get("metricas_segmento") or {}
            for metric in SEGMENT_METRICS:
                if metric not in contexts:
                    raise ValueError(f"Contexto ausente para {metric}")
                context = contexts[metric]
                value = row.get(metric)
                if value is None:
                    if context.get("availability") != "unavailable" or context.get("ordem_derivada") is not None:
                        raise ValueError(f"Missingness inválido em {metric}")
                else:
                    derived = context.get("ordem_derivada")
                    if context.get("availability") != "available" or not derived:
                        raise ValueError(f"Valor disponível sem ordem/contexto em {metric}")
                    if not (1 <= int(derived["posicao"]) <= int(derived["universo"])):
                        raise ValueError(f"Ordem derivada fora do universo em {metric}")
                    if derived.get("oficial") is not False:
                        raise ValueError("Ordem derivada não pode ser marcada como oficial")
