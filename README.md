# Comparador de Consórcios Data

Pipeline canônico de dados do **Comparador de Consórcios Sanida**.

## Estado do projeto

O `main` ainda alimenta a versão publicada existente. A reforma **V2** está sendo desenvolvida e homologada em paralelo, sem troca silenciosa do contrato de produção.

A V2 deve ajudar o usuário a entender **quem é a administradora**, **em quais segmentos há operação observada**, **quais sinais públicos existem sobre sua operação e reclamações** e **como esses sinais se comparam aos de outras administradoras**, sem transformar ausência de dados, porte ou número de filiais em selo artificial de qualidade.

Documentos centrais:

- [`docs/METODOLOGIA_V2.md`](docs/METODOLOGIA_V2.md) — contrato metodológico;
- [`config/methodology_v2.json`](config/methodology_v2.json) — política executável;
- [`docs/REMEDIATION_STATUS_V2.md`](docs/REMEDIATION_STATUS_V2.md) — C01–C16: implementado, parcial e pendente.

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
  --methodology config/methodology_v2.json \
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

O consolidado pode conter combinações raiz×segmento zeradas. Elas **não são tratadas como portfólio**. Um segmento só entra em `produtos.v2` e no `portfolio_observado` quando existe sinal operacional positivo. Na base auditada de maio/2026, isso reduz 762 combinações consolidadas para **368 observações operacionais em 124 raízes**.

### Missingness

A V2 preserva distinção entre zero, ausência, índice não divulgado e falta de vínculo. Não existe nota neutra por ausência, fallback de score legado ou redistribuição automática de pesos.

## Validação V2

O workflow `.github/workflows/11-validate-v2.yml` executa em PR e valida em **Python + PHP 8.2**:

- compilação e testes do builder;
- parsing estrito e distinção entre zero/ausência;
- integridade das chaves e dos contratos;
- exclusão de combinações zeradas do portfólio observado;
- reconciliação dos totais medidos na auditoria;
- ausência de posição BC fabricada;
- ausência de score/ranking geral na primeira versão V2;
- correspondência integral entre arquivos físicos, manifesto e SHA-256;
- rejeição de núcleo ausente/bytes adulterados/JSON não declarado;
- preservação de último sucesso após tentativa falha;
- lock concorrente e troca atômica de symlink.

Os artefatos de homologação são gerados em diretório isolado e enviados como artifact do workflow; o workflow não publica a V2 no HostGator.

## Publicação HostGator V2

A nova camada está versionada em `hostgator/v2/`:

- `consorcio-v2-lib.php` — validador e primitivas compartilhadas;
- `consorcio-pull-deploy-v2.php` — pull por commit imutável, staging, validação, swap e quarentena;
- `consorcio-validate-current-v2.php` — validação do `current-v2`;
- `consorcio-rollback-v2.php` — rollback somente para release previamente validada;
- `consorcio-v2-config.php` — contrato e caminhos da instalação paralela.

Pull, validação e rollback usam **o mesmo manifesto e o mesmo validador**. O pull resolve a ref remota para um SHA de commit antes de baixar qualquer arquivo, evitando combinar bytes de diferentes estados de `main`. `last_validation_attempt` e `last_validation_success` são estados separados.

`config/deploy_v2.json` permanece deliberadamente com `deploy_enabled=false`: os scripts já estão versionados e testados, mas ainda não foram instalados/homologados no HostGator nem conectados ao frontend público.

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

Uma PR verde da V2 prova o contrato do pipeline e da camada de publicação em fixtures. Ela **não prova, sozinha, que o frontend publicado já consome esse contrato**. O aceite final deve vincular commit, release, validação e consumidor da mesma geração no PHP 8.2 do HostGator.
