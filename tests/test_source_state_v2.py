import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PERSIST = ROOT / "scripts" / "persist_source_state.py"
FINALIZE = ROOT / "transform" / "finalize_v2_release.py"


def run(*args):
    return subprocess.run([sys.executable, *map(str, args)], check=True, capture_output=True, text=True)


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def test_persist_preserves_last_changed_when_unchanged(tmp_path):
    runtime1 = tmp_path / "runtime1.json"
    snapshot = tmp_path / "snapshot.json"
    content = tmp_path / "data.bin"
    state = tmp_path / "state.json"
    content.write_bytes(b"abc")
    snapshot.write_text(json.dumps({
        "metadata": {"collected_at": "2026-09-01T10:00:00+00:00"},
        "selected_resource": {"selected_candidate": {"yyyymm": "202607"}},
    }), encoding="utf-8")
    runtime1.write_text(json.dumps({
        "last_checked_at": "2026-09-01T10:00:00+00:00",
        "last_changed_at": "2026-09-01T10:00:00+00:00",
        "changed": True,
        "mode_used": "direct-content-download",
    }), encoding="utf-8")

    run(PERSIST, "--source", "bc_consorciobd", "--runtime", runtime1, "--snapshot", snapshot,
        "--content-file", content, "--state", state, "--collector-outcome", "success",
        "--accepted-mode", "direct-content-download")
    first = read(state)
    assert first["last_changed_at"] == "2026-09-01T10:00:00+00:00"
    assert first["competence"] == {"kind": "month", "value": "202607"}

    runtime2 = tmp_path / "runtime2.json"
    runtime2.write_text(json.dumps({
        "last_checked_at": "2026-09-02T10:00:00+00:00",
        "last_changed_at": None,
        "changed": False,
        "mode_used": "direct-content-download",
    }), encoding="utf-8")
    run(PERSIST, "--source", "bc_consorciobd", "--runtime", runtime2, "--snapshot", snapshot,
        "--content-file", content, "--state", state, "--collector-outcome", "success",
        "--accepted-mode", "direct-content-download")
    second = read(state)
    assert second["last_checked_at"] == "2026-09-02T10:00:00+00:00"
    assert second["last_successful_check_at"] == "2026-09-02T10:00:00+00:00"
    assert second["last_changed_at"] == "2026-09-01T10:00:00+00:00"
    assert second["changed_on_last_success"] is False


def test_failed_check_preserves_last_success_and_content(tmp_path):
    state = tmp_path / "state.json"
    state.write_text(json.dumps({
        "schema": "source-state.v1",
        "source": "bc_x",
        "last_checked_at": "2026-09-01T00:00:00+00:00",
        "last_check_status": "success",
        "last_successful_check_at": "2026-09-01T00:00:00+00:00",
        "last_changed_at": "2026-09-01T00:00:00+00:00",
        "changed_on_last_success": True,
        "mode_used": "ok",
        "content_sha256": "deadbeef",
        "competence": {"kind": "month", "value": "202607"},
        "snapshot_collected_at": "2026-09-01T00:00:00+00:00",
        "last_error": None,
        "provenance": {},
    }), encoding="utf-8")

    run(PERSIST, "--source", "bc_x", "--state", state, "--collector-outcome", "failure")
    payload = read(state)
    assert payload["last_check_status"] == "failure"
    assert payload["last_successful_check_at"] == "2026-09-01T00:00:00+00:00"
    assert payload["last_changed_at"] == "2026-09-01T00:00:00+00:00"
    assert payload["content_sha256"] == "deadbeef"
    assert payload["competence"]["value"] == "202607"


def test_semester_and_position_date_competence(tmp_path):
    for name, snapshot_payload, expected in [
        ("ranking", {"selected": {"ano": 2026, "periodo": 1, "periodicidade": "SEMESTRAL"}}, {"kind": "semester", "value": "2026-S1", "year": 2026, "period": 1}),
        ("filiais", {"records": [{"Posicao": "10/09/2026"}, {"Posicao": "10/09/2026"}]}, {"kind": "position_date", "value": "2026-09-10"}),
    ]:
        runtime = tmp_path / f"{name}-runtime.json"
        snapshot = tmp_path / f"{name}-snapshot.json"
        content = tmp_path / f"{name}.bin"
        state = tmp_path / f"{name}-state.json"
        runtime.write_text(json.dumps({"last_checked_at": "2026-09-12T00:00:00+00:00", "changed": True, "mode_used": "ok"}), encoding="utf-8")
        snapshot.write_text(json.dumps(snapshot_payload), encoding="utf-8")
        content.write_bytes(name.encode())
        run(PERSIST, "--source", name, "--runtime", runtime, "--snapshot", snapshot,
            "--content-file", content, "--state", state, "--collector-outcome", "success", "--accepted-mode", "ok")
        assert read(state)["competence"] == expected


def test_finalizer_allows_degraded_check_but_requires_last_success(tmp_path):
    dist = tmp_path / "dist"
    (dist / "global").mkdir(parents=True)
    meta = {
        "pipeline_version": "4.0.1",
        "source_fingerprint": "src",
        "methodology_sha256": "method",
        "artifacts": {"global": [], "seo": []},
    }
    (dist / "global" / "meta.json").write_text(json.dumps(meta), encoding="utf-8")

    state = tmp_path / "source.json"
    state.write_text(json.dumps({
        "schema": "source-state.v1",
        "source": "bc_x",
        "last_checked_at": "2026-09-13T00:00:00+00:00",
        "last_check_status": "failure",
        "last_successful_check_at": "2026-09-12T00:00:00+00:00",
        "last_changed_at": "2026-09-01T00:00:00+00:00",
        "changed_on_last_success": False,
        "mode_used": "ok",
        "content_sha256": "abc",
        "competence": {"kind": "month", "value": "202607"},
        "snapshot_collected_at": "2026-09-01T00:00:00+00:00",
        "last_error": "collector_outcome=failure",
    }), encoding="utf-8")
    cfg = tmp_path / "provenance.json"
    cfg.write_text(json.dumps({"required_sources": {"bc_x": {"state_file": str(state), "role": "test"}}}), encoding="utf-8")

    run(FINALIZE, "--dist-base", dist, "--provenance-config", cfg)
    out = read(dist / "global" / "meta.json")
    assert out["backend_release"]["publication_eligible"] is True
    assert out["freshness"]["degraded_sources"] == ["bc_x"]
    assert out["source_status"]["bc_x"]["last_successful_check_at"] == "2026-09-12T00:00:00+00:00"


def test_hostgator_publication_failure_does_not_poison_current_validation_state():
    source = (ROOT / "hostgator" / "v2" / "consorcio-pull-deploy-v2.php").read_text(encoding="utf-8")
    assert "last_publication_attempt.json" in source
    assert "v2_record_validation($config, $failure)" not in source
