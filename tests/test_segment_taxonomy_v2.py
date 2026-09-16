import csv
import importlib.util
import io
import json
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TAXONOMY_SCRIPT = ROOT / "transform" / "segment_taxonomy_v2.py"
COLLECTOR_SCRIPT = ROOT / "collectors" / "bc_segment_taxonomy.py"

spec = importlib.util.spec_from_file_location("segment_taxonomy_v2", TAXONOMY_SCRIPT)
taxonomy = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(TAXONOMY_SCRIPT.parent))
spec.loader.exec_module(taxonomy)

collector_spec = importlib.util.spec_from_file_location("bc_segment_taxonomy", COLLECTOR_SCRIPT)
collector = importlib.util.module_from_spec(collector_spec)
collector_spec.loader.exec_module(collector)


def payload_with_code_7(status="ok_with_additions"):
    return {
        "contract": "segment-taxonomy.v1",
        "source": "bc_consorciobd_segment_taxonomy",
        "source_url": "https://www.bcb.gov.br/example",
        "status": status,
        "auto_added_codes": ["7"],
        "missing_codes": [],
        "renamed_codes": [],
        "items": [
            {
                "codigo": "1",
                "key": "imobiliario",
                "label_oficial": "bens imóveis",
                "label_exibicao": "Consórcio de imóveis",
            },
            {
                "codigo": "7",
                "key": "novo_segmento_oficial",
                "label_oficial": "novo segmento oficial",
                "label_exibicao": "Novo segmento oficial",
            },
        ],
    }


def test_taxonomy_load_and_dynamic_segment_map(tmp_path):
    path = tmp_path / "taxonomy.json"
    path.write_text(json.dumps(payload_with_code_7(), ensure_ascii=False), encoding="utf-8")

    loaded = taxonomy.load_taxonomy(path)
    mapped = taxonomy.segment_map(loaded)

    assert mapped["1"] == ("imobiliario", "Consórcio de imóveis")
    assert mapped["7"] == ("novo_segmento_oficial", "Novo segmento oficial")
    assert taxonomy.taxonomy_codes(loaded) == {"1", "7"}


def test_additive_taxonomy_is_release_safe():
    payload = payload_with_code_7()
    taxonomy.assert_release_safe(payload, {"1", "7"})


def test_structural_taxonomy_is_rejected():
    payload = payload_with_code_7(status="structural_change_detected")
    try:
        taxonomy.assert_release_safe(payload, {"1", "7"})
    except ValueError as exc:
        assert "mudança estrutural" in str(exc)
    else:
        raise AssertionError("mudança estrutural deveria bloquear a release")


def test_observed_segment_codes_keeps_future_code(tmp_path):
    archive = tmp_path / "Consorcios.zip"
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=["CNPJ da Administradora", "Data Base", "Código do Segmento", "Quantidade de Cotas Ativas em Dia"],
        delimiter=";",
    )
    writer.writeheader()
    writer.writerow({
        "CNPJ da Administradora": "12345678000190",
        "Data Base": "202701",
        "Código do Segmento": "1",
        "Quantidade de Cotas Ativas em Dia": "10",
    })
    writer.writerow({
        "CNPJ da Administradora": "12345678000190",
        "Data Base": "202701",
        "Código do Segmento": "7",
        "Quantidade de Cotas Ativas em Dia": "5",
    })
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("202701Segmentos_Consolidados.csv", output.getvalue().encode("utf-8"))

    assert taxonomy.observed_segment_codes(archive) == {"1", "7"}


def test_official_page_parser_accepts_additive_code():
    html = b"""
    <html><body>
      <h2>Segmentos Consolidados</h2>
      <p>* 1 = bens imoveis; 2 = tratores e maquinas; 3 = veiculos leves;
         4 = motocicletas e motonetas; 5 = outros bens moveis duraveis;
         6 = servicos turisticos; 7 = novo segmento oficial</p>
      <h2>2. Dados por Unidade da Federacao</h2>
    </body></html>
    """
    parsed = collector.parse_official_segments(html)
    assert parsed["1"] == "bens imoveis"
    assert parsed["6"] == "servicos turisticos"
    assert parsed["7"] == "novo segmento oficial"


def test_key_collision_is_deterministic():
    used = {"novo_segmento"}
    assert collector.unique_key("Novo segmento", "7", used) == "novo_segmento_7"
