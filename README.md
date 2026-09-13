# Comparador de Consórcios Data

Pipeline canônico de dados do **Comparador de Consórcios Sanida**.

## Estado do projeto

O `main` ainda alimenta a versão publicada existente. A reforma **V2** está sendo desenvolvida e homologada sem troca silenciosa do contrato de produção.

A V2 parte de uma premissa simples: o comparador deve ajudar o usuário a entender **quem é a administradora**, **em quais segmentos há operação observada**, **quais sinais públicos existem sobre sua operação e reclamações** e **como esses sinais se comparam aos de outras administradoras**, sem transformar ausência de dados, porte ou número de filiais em um selo artificial de qualidade.

A especificação normativa da V2 está em [`docs/METODOLOGIA_V2.md`](docs/METODOLOGIA_V2.md). A configuração executável correspondente está em [`config/methodology_v2.json`](config/methodology_v2.json).

## Fontes e camadas

O repositório mantém três camadas principais:

1. `data/raw/` — cópia dos insumos coletados;
2. `data/stage/` — normalizações próprias dos coletores;
3. `data/dist/` — artefatos públicos consumíveis pelo frontend/servidor.

As fontes atualmente usadas incluem cadastro e filiais de administradoras do Banco Central, ConsorcioBD mensal e trimestral, ranking de reclamações do Banco Central, séries SGS e contexto setorial complementar.

## Builder V2

O builder em homologação é:

```bash
python transform/build_read_models_v2.py \
  --config config/sources.json \
  --seo-routes config/seo_routes.json
```

Ele produz os contratos:

- `instituicoes.v2` — identidade cadastral atual e presença informativa;
- `administradoras.v2` — perfil consolidado, cobertura e leitura limitada das evidências;
- `produtos.v2` — segmentos com operação observada por raiz + competência + segmento;
- `rankings.v2` — reclamações sem inventar posição oficial;
- `segmentos.v2` — contexto por segmento;
- `comparacoes.v2` — dimensões comparáveis por segmento, **sem ranking geral**;
- `ofertas.v2` — contrato comercial separado, vazio enquanto não houver fonte própria validada.

### Fonte canônica operacional

Para estoques, fluxos e taxa de administração, a V2 usa `Segmentos_Consolidados` do ConsorcioBD na competência selecionada. Arquivos de grupos não são somados ao consolidado; servem apenas a estatísticas de grupo compatíveis, como prazo e valor médio do bem, e à reconciliação.

### Missingness

A V2 preserva distinção entre zero, ausência, índice não divulgado e falta de vínculo. Não existe nota neutra por ausência, fallback de score legado ou redistribuição automática de pesos.

## Validação V2

O workflow `.github/workflows/11-validate-v2.yml` executa em PR e valida:

- compilação e testes do builder;
- parsing estrito e distinção entre zero/ausência;
- integridade das chaves e dos contratos;
- reconciliação dos totais imobiliários medidos na auditoria;
- ausência de posição BC fabricada;
- ausência de score/ranking geral na primeira versão V2;
- correspondência integral entre arquivos físicos, manifesto e SHA-256.

Os artefatos de homologação são gerados em diretório isolado e enviados como artifact do workflow; o workflow não publica a V2 no HostGator.

## Publicação e migração

A publicação atual usa estratégia pull no HostGator. O frontend PHP e os scripts locais de pull/validação/rollback **não vivem neste repositório**.

Por isso, `config/deploy_v2.json` é deliberadamente um contrato de **homologação com `deploy_enabled=false`**. A ativação da V2 depende de:

- migrar o consumidor PHP para os contratos V2;
- fazer pull, validador e rollback compartilharem o mesmo manifesto e as mesmas invariantes;
- separar tentativa de validação de último sucesso;
- impedir fallback silencioso para score legado;
- homologar uma release concreta no PHP 8.2 usado pelo ambiente.

Até essa migração ser concluída, a V2 não deve substituir automaticamente os artefatos de produção.

## Workflows existentes

- `01` — cadastro BC;
- `02` — filiais BC;
- `03` — séries SGS;
- `04` — ConsorcioBD mensal;
- `05` — ConsorcioBD trimestral;
- `06` — ranking de reclamações;
- `07` — ABAC/contexto;
- `08` — builder legado atualmente ligado à produção;
- `09` — readiness do pull HostGator atual;
- `10` — validação HostGator atual;
- `11` — validação isolada da reforma V2.

## Princípio de segurança da migração

Uma PR verde da V2 prova o contrato do pipeline e de seus artefatos de homologação. Ela **não prova, sozinha, que o frontend publicado já consome esse contrato**. O aceite final deve vincular commit, release, validação e consumidor da mesma geração.
