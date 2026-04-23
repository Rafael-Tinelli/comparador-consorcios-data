# Comparador de Consórcios Data

Pipeline de coleta, normalização, relacionamento, validação e geração de artefatos para o projeto **Comparador de Consórcios**.

## Objetivo

Este repositório existe para:

- coletar dados oficiais e complementares sobre administradoras, produtos, séries e contexto setorial;
- normalizar e relacionar essas fontes em modelos de leitura prontos para consumo;
- gerar artefatos leves em JSON para o cluster `/financas/consorcio/`;
- alimentar páginas SEO e componentes comparativos sem ETL pesado no HostGator;
- manter a arquitetura evergreen, barata, rápida e de baixa manutenção.

## Arquitetura resumida

A arquitetura v1 segue esta lógica:

1. **GitHub Actions** executa as coletas.
2. Os dados brutos vão para `data/raw/`.
3. Os dados intermediários e consolidados passam por `data/stage/`.
4. As transformações geram os artefatos finais em `data/dist/`.
5. O **HostGator consome os JSONs finais por estratégia pull**, preferencialmente via cron, sem depender de deploy ativo por SSH a partir do GitHub.
6. O frontend PHP do site lê os JSONs localmente e renderiza HTML indexável no servidor.

## Estratégia de publicação recomendada

A estratégia preferencial de publicação é:

1. O GitHub gera os arquivos finais em `data/dist/`.
2. O GitHub também pode expor um `manifest.json` ou metadados de versão/hash.
3. Um **cron job no HostGator** consulta esse manifest.
4. Se houver nova versão, o servidor baixa os arquivos para uma pasta temporária.
5. O servidor valida os arquivos baixados.
6. O servidor publica a nova release de forma atômica, atualizando o apontamento de `current/`.

Essa abordagem reduz dependência de SSH com escrita no servidor e separa claramente:

- **GitHub** = coleta, tratamento e build
- **HostGator** = consumo e publicação local

## Saída final esperada

Os artefatos finais ficam em:

- `data/dist/global/`
- `data/dist/seo/`

Atualmente, os artefatos globais incluem:

- `instituicoes.json`
- `produtos.json`
- `rankings.json`
- `series.json`
- `cenarios.json`
- `autocomplete.json`
- `meta.json`

Os artefatos SEO ficam em `data/dist/seo/`.

No HostGator, a publicação final esperada fica em:

- `/home1/SEU_USUARIO/consorcio-data/current/global/`
- `/home1/SEU_USUARIO/consorcio-data/current/seo/`

O frontend público permanece em:

- `/public_html/financas/consorcio/`

## Estrutura do repositório

```text
comparador-consorcios-data/
│
├── .github/
│   └── workflows/
│       ├── 01-coleta-bc-cadastro.yml
│       ├── 02-coleta-bc-filiais.yml
│       ├── 03-coleta-bc-series.yml
│       ├── 04-coleta-consorciobd.yml
│       ├── 05-coleta-consorciobd-trimestral.yml
│       ├── 06-coleta-ranking-reclamacoes.yml
│       ├── 07-coleta-abac.yml
│       ├── 08-build-read-models.yml
│       ├── 09-deploy-hostgator.yml
│       └── 10-validate-hostgator.yml
│
├── collectors/
│   ├── bc_cadastro_admins.py
│   ├── bc_filiais.py
│   ├── bc_sgs_series.py
│   ├── bc_consorciobd.py
│   ├── bc_consorciobd_trimestral.py
│   ├── bc_ranking_reclamacoes.py
│   └── abac_boletim.py
│
├── transform/
│   ├── normalize_instituicoes.py
│   ├── normalize_series.py
│   ├── normalize_rankings.py
│   ├── normalize_produtos.py
│   ├── build_autocomplete.py
│   ├── build_landings_data.py
│   └── build_read_models.py
│
├── shared/
├── config/
├── schemas/
├── data/
│   ├── raw/
│   ├── stage/
│   ├── runtime/
│   └── dist/
│       ├── global/
│       └── seo/
│
├── tests/
├── requirements.txt
├── README.md
└── .gitignore
