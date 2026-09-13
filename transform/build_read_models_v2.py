#!/usr/bin/env python3
"""Compatibilidade de importação para o núcleo de transformação V2.

O executável canônico da release V2 é ``transform/build_release_v2.py``.
Este módulo existe somente para preservar imports já usados pelos testes e pelo
builder canônico enquanto o núcleo de domínio é separado do antigo CLI.

Executar este arquivo diretamente é proibido para evitar a reintrodução do
builder anterior, que ainda continha a antiga superfície de SEO.
"""
from __future__ import annotations

from _read_models_v2_core_legacy import *  # noqa: F401,F403


if __name__ == "__main__":
    raise SystemExit(
        "build_read_models_v2.py não é mais um CLI. "
        "Use transform/build_release_v2.py para gerar a release V2 data-only."
    )
