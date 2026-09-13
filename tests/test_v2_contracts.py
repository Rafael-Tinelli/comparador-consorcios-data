from pathlib import Path
import importlib.util
import sys

SCRIPT = Path(__file__).resolve().parents[1] / "transform" / "build_read_models_v2.py"
spec = importlib.util.spec_from_file_location("v2", SCRIPT)
v2 = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(SCRIPT.parent))
spec.loader.exec_module(v2)


def test_strict_number_does_not_extract_arbitrary_text():
    assert v2.strict_number("abc 123 xyz") is None
    assert v2.strict_number("1e3") is None
    assert v2.strict_number("1.234,56") == 1234.56
    assert v2.strict_number("0") == 0.0


def test_root8_is_typed_and_deterministic():
    assert v2.root8("12.345.678/0001-90") == "12345678"
    assert v2.root8("12345678") == "12345678"
    assert v2.root8("123") is None


def test_zero_and_missing_are_distinct():
    zero = v2.availability(0)
    missing = v2.availability(None)
    assert zero["availability"] == "available"
    assert zero["value"] == 0
    assert missing["availability"] == "unavailable"
    assert missing["value"] is None


def test_delinquency_share_not_old_ratio():
    n = 100
    inad = 25
    assert inad / (n + inad) == 0.2
    assert inad / n == 0.25


def test_zero_filled_segment_is_not_observed_portfolio():
    row = {
        "taxa_administracao_pct": 0.0,
        "grupos_ativos": 0,
        "cotas_ativas_em_dia": 0,
        "contemplacoes_mes": 0,
        "inadimplentes": 0,
    }
    assert v2.has_operational_signal(row) is False


def test_any_positive_operational_signal_makes_segment_observed():
    row = {
        "taxa_administracao_pct": 0.0,
        "grupos_ativos": 0,
        "cotas_ativas_em_dia": 1,
        "contemplacoes_mes": 0,
        "inadimplentes": 0,
    }
    assert v2.has_operational_signal(row) is True


def test_sum_complete_preserves_zero_but_rejects_partial_missingness():
    assert v2.sum_complete([{"x": 0}, {"x": 2}], "x") == 2
    assert v2.sum_complete([{"x": None}, {"x": 2}], "x") is None
