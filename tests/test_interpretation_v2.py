from copy import deepcopy
from pathlib import Path
import importlib.util
import sys

SCRIPT = Path(__file__).resolve().parents[1] / "transform" / "interpretation_v2.py"
spec = importlib.util.spec_from_file_location("interpretation_v2", SCRIPT)
iv2 = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(SCRIPT.parent))
spec.loader.exec_module(iv2)


def methodology():
    return {
        "relative_interpretation": {
            "enabled": True,
            "contract": iv2.INTERPRETATION_CONTRACT,
            "general_ranking": False,
        }
    }


def sample_models():
    admins = []
    rows = []
    values = [
        ("00000001", "A", 10.0, 100, 5, 0.05, 10.0),
        ("00000002", "B", 20.0, 200, 10, 0.10, 20.0),
        ("00000003", "C", 30.0, 300, 15, 0.15, None),
        ("00000004", "D", 40.0, 400, 20, 0.20, None),
    ]
    for root, name, rate, quotas, contemp, inad, complaint in values:
        rep_status = "divulgado" if complaint is not None else "nao_divulgado_pela_fonte"
        admins.append(
            {
                "cnpj_root": root,
                "nome": name,
                "identidade": {"cadastro_atual_bcb": True},
                "portfolio_observado": {
                    "segmentos": [
                        {
                            "codigo": "1",
                            "key": "imobiliario",
                            "label": "Consórcio de imóveis",
                            "competencia": "202605",
                            "indicadores": {
                                "taxa_administracao_pct": rate,
                                "grupos_ativos": 1,
                                "cotas_ativas_em_dia": quotas,
                                "contemplacoes_mes": contemp,
                                "inadimplencia_participacao": inad,
                                "prazo_medio_grupos_meses": None,
                                "valor_medio_bem_grupos": None,
                            },
                        }
                    ]
                },
                "comparabilidade": {"ranking_geral_publicavel": False},
                "reclamacoes_bcb": {
                    "indice_bc": complaint,
                    "indice_status": rep_status,
                },
                "leitura_confiabilidade": {},
            }
        )
        rows.append(
            {
                "cnpj_root": root,
                "nome": name,
                "taxa_administracao_pct": rate,
                "grupos_ativos": 1,
                "cotas_ativas_em_dia": quotas,
                "contemplacoes_mes": contemp,
                "inadimplencia_participacao": inad,
                "indice_reclamacoes_bcb": complaint,
                "indice_reclamacoes_status": rep_status,
            }
        )

    return {
        "administradoras": {
            "metadata": {"contract": "administradoras.v2"},
            "items": admins,
        },
        "segmentos": {
            "metadata": {"contract": "segmentos.v2"},
            "items": [
                {
                    "segmento": "imobiliario",
                    "label": "Consórcio de imóveis",
                    "codigo": "1",
                    "competencia": "202605",
                }
            ],
        },
        "comparacoes": {
            "metadata": {"contract": "comparacoes.v2", "ranking_geral": False},
            "items": [
                {
                    "segmento": "imobiliario",
                    "label": "Consórcio de imóveis",
                    "competencia": "202605",
                    "administradoras": rows,
                }
            ],
        },
    }


def test_distribution_and_dense_rank_are_deterministic():
    stats = iv2.distribution([1, 2, 3, 4])
    assert stats["q1"] == 1.75
    assert stats["mediana"] == 2.5
    assert stats["q3"] == 3.25
    ranks = iv2.dense_rank({"a": 1, "b": 2, "c": 2}, order="asc")
    assert ranks["a"]["posicao"] == 1
    assert ranks["b"]["posicao"] == ranks["c"]["posicao"] == 2
    assert ranks["a"]["universo"] == 3


def test_enrichment_creates_context_without_general_score():
    models = iv2.enrich_models(sample_models(), methodology())
    iv2.validate_interpretations(models, methodology())

    assert (
        models["administradoras"]["metadata"]["interpretation_contract"]
        == iv2.INTERPRETATION_CONTRACT
    )
    assert models["comparacoes"]["items"][0]["interpretacao"]["general_ranking"] is False

    row_a = models["comparacoes"]["items"][0]["administradoras"][0]
    tax = row_a["interpretacao_relativa"]["metricas_segmento"]["taxa_administracao_pct"]
    assert tax["faixa_distribuicao"] == "quartil_inferior"
    assert tax["ordem_derivada"]["posicao"] == 1
    assert tax["ordem_derivada"]["oficial"] is False
    assert "menores taxas" in tax["destaque"]

    scale = row_a["interpretacao_relativa"]["metricas_segmento"]["cotas_ativas_em_dia"]
    assert scale["ordem_derivada"]["sentido"] == "maior_primeiro"
    assert "porte não é qualidade" in scale["destaque"]

    assert "score" not in row_a
    assert (
        models["comparacoes"]["items"][0]["interpretacao"]["contemplacao"]["conclusao"]
        == "nao_inferivel"
    )


def test_missing_complaints_never_receive_rank_or_zero():
    models = iv2.enrich_models(sample_models(), methodology())
    admin_c = next(
        x for x in models["administradoras"]["items"] if x["cnpj_root"] == "00000003"
    )
    context = admin_c["interpretacao_relativa"]["reclamacoes_bcb"]
    assert context["availability"] == "unavailable"
    assert context["ordem_derivada"] is None
    assert context["reason"] == "nao_divulgado_pela_fonte"
    assert "não equivale a zero" in context["nota"]


def test_trust_reading_is_evidence_reading_not_certification():
    models = iv2.enrich_models(sample_models(), methodology())
    admin_a = models["administradoras"]["items"][0]
    reading = admin_a["leitura_confiabilidade"]
    assert reading["resposta_chave"] == "evidencia_ampla_para_triagem"
    assert "não certifica confiabilidade" in reading["texto"]
    assert reading["sinais"]["cadastro_atual_bcb"] is True
    assert reading["sinais"]["operacao_mensal_observada"] is True
    assert reading["sinais"]["indice_reclamacoes_bcb_divulgado"] is True


def test_enrichment_does_not_mutate_input():
    original = sample_models()
    before = deepcopy(original)
    iv2.enrich_models(original, methodology())
    assert original == before
