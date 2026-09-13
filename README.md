# Comparador de Consórcios Data

Pipeline canônico de dados do **Comparador de Consórcios Sanida**.

## Estado do projeto

O `main` ainda alimenta a versão publicada existente. A reforma **V2** está sendo desenvolvida e homologada em paralelo, sem troca silenciosa do contrato de produção.

A V2 deve ajudar o usuário a entender **quem é a administradora**, **em quais segmentos há operação observada**, **quais sinais públicos existem sobre sua operação e reclamações** e **como esses sinais se comparam aos de outras administradoras**, sem transformar ausência de dados, porte ou número de filiais em selo artificial de qualidade.

Documentos centrais:

- [`docs/METODOLOGIA_V2.md`](docs/METODOLOGIA_V2.md) — contrato metodológico;
- [`config/methodology_v2.json`](config/methodology_v2.json) — política executável;
- [`config/provenance_v2.json`](config/provenance_v2.json) — contrato de consulta, mudança e competência das fontes críticas;
- [`docs/REMEDIATION_STATUS_V2.md`](docs/REMEDIATION_STATUS_V2.md) — C01–C16: implementado, parcial e pendente.

## Fontes e camadas

O repositório separa explicitamente dados, estado de coleta e publicação:

1. `data/raw/` — cópia dos insumos coletados;
2. `data/stage/` — normalizações próprias dos coletores;
3. `data/source_state/` — estado durável de cada fonte crítica (`last_checked_at`, último sucesso, última mudança, competência, hash e erro da última tentativa);
4. `data/dist/` — artefatos da geração legada atualmente ligada à produção;
5. `data/dist-v2/` — artefatos canônicos do backend V2.

As fontes atualmente usadas incluem cadastro e filiais de administradoras do Banco Central, ConsorcioBD mensal e trimestral, ranking de reclamações do Banco Central, séries SGS e contexto setorial complementar.

## Builder V2

O builder é:

```bash
python transform/build_read_models_v2.py \
  --config config/sources.json \
  --methodology config/methodology_v2.json \
  --seo-routes config/seo_routes.json
```

No workflow canônico, `dist_base_dir` é sobrescrito para `data/dist-v2`. Depois do build, `transform/finalize_v2_release.py` incorpora a proveniência persistente ao `global/meta.json` sem confundir momento de geração com atualidade da fonte.

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

### Atualidade e proveniência — C09

`generated_at` significa apenas **quando os read models foram gerados**. Ele não pode ser apresentado como data de atualização da fonte.

Os coletores críticos de cadastro, filiais, ConsorcioBD mensal e reclamações mantêm arquivos `source-state.v1`. Uma consulta bem-sucedida sem mudança avança `last_checked_at` e `last_successful_check_at`, mas preserva `last_changed_at`. Uma consulta com falha registra a falha sem apagar o último conteúdo aprovado, seu hash ou sua competência.

O `meta.json` da release transporta esses estados em `source_status`, além de `freshness.degraded_sources`. Assim, conteúdo válido pode continuar disponível após uma falha transitória de coleta, mas a degradação fica explícita e não é mascarada por uma nova geração.

## Validação V2

O workflow `.github/workflows/11-validate-v2.yml` executa em PR e valida em **Python + PHP 8.2**:

- compilação e testes do builder e da persistência de proveniência;
- parsing estrito e distinção entre zero/ausência;
- integridade das chaves e dos contratos;
- exclusão de combinações zeradas do portfólio observado;
- reconciliação dos totais medidos na auditoria por baseline de regressão separado;
- ausência de posição BC fabricada;
- ausência de score/ranking geral na primeira versão V2;
- correspondência integral entre arquivos físicos, manifesto e SHA-256;
- estado persistente das fontes críticas e contrato `comparador-v2-release.v1`;
- rejeição de núcleo ausente/bytes adulterados/JSON não declarado/proveniência ausente;
- preservação de último sucesso após tentativa falha;
- lock concorrente e troca atômica de symlink;
- separação entre tentativa de publicação e estado de validação da release ativa.

Os artefatos de homologação são gerados em diretório isolado e enviados como artifact do workflow; o workflow de PR não publica a V2 no HostGator.

## Geração canônica V2

O workflow `.github/workflows/12-build-publish-v2.yml` é a geração recorrente do backend V2. Ele:

1. testa o backend;
2. cria somente os estados de fonte ainda ausentes durante a migração inicial;
3. gera `data/dist-v2`;
4. anexa a proveniência ao manifesto;
5. executa os gates estruturais e PHP 8.2;
6. recusa publicação se a branch tiver avançado durante o build;
7. versiona `data/dist-v2` e `data/source_state` no mesmo commit de publicação.

Os números exatos da Auditoria 2 ficam em `config/audit_baseline_v2.json` e são usados como teste de regressão da fixture auditada, **não como limites fixos do workflow recorrente**. O pipeline de produção deve aceitar mudanças legítimas nas fontes sem exigir que o mercado permaneça congelado nos totais de maio/2026.

## Publicação HostGator V2

A nova camada está versionada em `hostgator/v2/`:

- `consorcio-v2-lib.php` — validador e primitivas compartilhadas;
- `consorcio-v2-release-gate.php` — gate do contrato de release e proveniência;
- `consorcio-pull-deploy-v2.php` — pull por commit imutável, staging, validação, swap e quarentena;
- `consorcio-validate-current-v2.php` — validação do `current-v2`;
- `consorcio-rollback-v2.php` — rollback somente para release previamente validada;
- `consorcio-v2-config.php` — contrato e caminhos da instalação paralela.

Pull, validação e rollback usam **o mesmo manifesto e o mesmo gate de backend**. O pull resolve a ref remota para um SHA de commit antes de baixar qualquer arquivo, evitando combinar bytes de diferentes estados de `main`. `last_validation_attempt` e `last_validation_success` são estados da validação da release; `last_publication_attempt` e `last_publication_success` são estados separados da tentativa de publicação. Uma falha de download ou de release candidata não deve fazer uma release ativa e saudável parecer inválida.

O HostGator V2 lê exclusivamente `data/dist-v2`. `config/deploy_v2.json` permanece deliberadamente com `deploy_enabled=false`: o backend está preparado para homologação, mas os scripts ainda não foram instalados/testados no HostGator real nem conectados ao frontend público.

## Workflows existentes

- `01` — cadastro BC + estado persistente da fonte;
- `02` — filiais BC + estado persistente da fonte;
- `03` — séries SGS;
- `04` — ConsorcioBD mensal + estado persistente da fonte;
- `05` — ConsorcioBD trimestral;
- `06` — ranking de reclamações + estado persistente da fonte;
- `07` — ABAC/contexto;
- `08` — builder legado atualmente ligado à produção;
- `09` — readiness do pull HostGator atual;
- `10` — validação HostGator atual;
- `11` — validação isolada/regressão da reforma V2;
- `12` — geração e publicação canônica do backend V2 no repositório.

## Princípio de segurança da migração

Uma PR verde da V2 prova o contrato do pipeline e da camada de publicação em fixtures. Ela **não prova, sozinha, que o frontend publicado já consome esse contrato**. O aceite final de produção deve vincular commit, release, validação e consumidor da mesma geração no PHP 8.2 do HostGator.
