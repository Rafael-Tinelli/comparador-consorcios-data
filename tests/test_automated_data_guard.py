"""Regression tests for the automated GitHub data-push allowlist."""
from scripts.guard_automated_data_commit import ALLOWED_FILES, unexpected_paths


def test_allow_expected_files_for_every_automated_writer():
    for workflow, allowed_paths in ALLOWED_FILES.items():
        assert allowed_paths, workflow
        assert unexpected_paths(workflow, allowed_paths) == []


def test_reject_code_and_configuration_in_all_writers():
    forbidden = [
        "collectors/bc_consorciobd.py",
        "transform/build_release_v2.py",
        "scripts/guard_automated_data_commit.py",
        ".github/workflows/06-build-publish-v2.yml",
        "config/methodology_v2.json",
        "README.md",
    ]
    for workflow in ALLOWED_FILES:
        assert unexpected_paths(workflow, forbidden) == sorted(forbidden)


def test_each_writer_is_limited_to_its_own_data_files():
    assert unexpected_paths("cadastro", ["data/stage/filiais/filiais.json"]) == [
        "data/stage/filiais/filiais.json"
    ]
    assert unexpected_paths("filiais", ["data/stage/cadastro/instituicoes_cadastro.json"]) == [
        "data/stage/cadastro/instituicoes_cadastro.json"
    ]
    assert unexpected_paths("consorciobd", ["data/raw/bc/ranking_reclamacoes/latest_source.csv"]) == [
        "data/raw/bc/ranking_reclamacoes/latest_source.csv"
    ]
    assert unexpected_paths("reclamacoes", ["data/raw/bc/consorciobd/latest_source.bin"]) == [
        "data/raw/bc/consorciobd/latest_source.bin"
    ]


def test_release_cannot_write_raw_inputs_or_unlisted_artifacts():
    assert unexpected_paths(
        "release",
        [
            "data/raw/bc/consorciobd/latest.json",
            "data/dist-v2/seo/legacy.json",
            "data/dist-v2/global/unreviewed.json",
        ],
    ) == sorted(
        [
            "data/raw/bc/consorciobd/latest.json",
            "data/dist-v2/seo/legacy.json",
            "data/dist-v2/global/unreviewed.json",
        ]
    )


def test_prefix_collisions_and_path_escape_are_rejected():
    assert unexpected_paths("release", ["data/dist-v2/global/meta.json.bak", "../README.md"]) == [
        "../README.md",
        "data/dist-v2/global/meta.json.bak",
    ]


def test_empty_index_is_safe():
    assert unexpected_paths("release", []) == []
