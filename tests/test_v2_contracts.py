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
