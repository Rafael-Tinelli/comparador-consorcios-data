# Comparador de Consórcios Data

Pipeline de coleta, normalização, relacionamento, validação e publicação de dados para o projeto **Comparador de Consórcios**.

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
5. O deploy envia apenas os JSONs finais ao HostGator.
6. O frontend PHP do site lê os JSONs localmente e renderiza HTML indexável no servidor.

## Saída final esperada

Os artefatos finais ficam em:

- `data/dist/global/`
- `data/dist/seo/`

No HostGator, a publicação final fica em:

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
├── transform/
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
