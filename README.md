# Comparador de Consórcios Data

Pipeline canônico de dados do **Comparador de Consórcios Sanida**.

> **Papel deste README**
>
> Este arquivo é o memorial técnico e operacional do projeto. O código e as configurações são a verdade executável; o README registra as fronteiras, invariantes, estados e decisões que não podem ser simplificados silenciosamente.

## 1. Regra de preservação arquitetural

Qualquer alteração material em coletores, workflows, contratos, papéis das fontes, proveniência, publicação, rollback, caminhos de artefatos ou fronteiras V1/V2 deve atualizar este README **na mesma PR**.

Regras permanentes:

- não remover V1 enquanto a migração V2 não tiver encerramento explícito e comprovado em produção;
- não confundir `generated_at`, consulta da fonte, mudança do conteúdo, competência do dado e publicação da release;
- não transformar ausência em zero, nota neutra ou redistribuição de peso;
- não reintroduzir score/ranking geral sem nova metodologia versionada e nova auditoria;
- não alterar `current-v2`, manifesto, validador ou state de forma independente;
- não combinar artefatos construídos de commits diferentes;
- não enfraquecer hash, tamanho, contrato, staging, lock, quarentena, swap atômico ou rollback;
- simplificação é desejável quando reduz acoplamento sem reduzir integridade, rastreabilidade ou capacidade de auditoria.

## 2. Estado atual

A V1 continua sendo a produção pública. O backend V2 e a camada paralela de publicação HostGator foram homologados em 13/09/2026 sob o contrato histórico `comparador-v2-release.v1`.

Durante C15 foi confirmado no HostGator real que o site já possui uma arquitetura editorial própria, baseada em `config-site.php`, `head-global.php`, menu, footer e demais includes. Também foi confirmado que o comparador central é o consumidor específico da camada de dados, enquanto as páginas filhas do cluster seguem o padrão normal do site.

Por decisão arquitetural, a release V2 passa a ser **data-only**:

- o repositório de dados publica somente contratos de dados e proveniência;
- SEO editorial não é mais artefato da release de dados;
- `title`, `description`, `canonical`, H1, breadcrumbs, FAQ e demais decisões editoriais pertencem ao frontend/site;
- `config-site.php` permanece autoridade para arquitetura global, sitemap e navegação;
- o frontend deve usar os includes padrão da Sanida, sem um segundo catálogo de rotas SEO dentro do pipeline de dados.

A mudança criou o contrato `comparador-v2-release.v2`. A última publicação canônica registrada continua sendo o pipeline `4.1.0`: o workflow 12 #5 publicou em `main` o commit `5664bd5360671ad94485aa8b11ab90fc142e2732`, com sete artefatos de dados não-meta, nenhum JSON SEO físico, `source_fingerprint=72456642883fa6cc56f9169d2572394e899ad3cec2d86816f7e22e63f8681780` e `release_fingerprint=9cd474256d02174d0033fb2e1398499c3d26fa9dc130db4ed584339b54f41cb8`.

A extensão de interpretação relativa introduz metodologia `2.1.0`, pipeline `4.2.0` e subcontrato embutido `interpretacao-relativa.v1`. Ela é aditiva aos mesmos sete artefatos V2; não cria score geral, novo JSON global nem SEO de backend. Só deve ser considerada geração canônica depois de PR verde, merge e nova execução válida do workflow 12.

A homologação operacional de 13/09/2026 sob `comparador-v2-release.v1` continua válida como prova histórica do mecanismo. `config/deploy_v2.json` permanece com `deploy_enabled=false`.

## 3. Responsabilidades por camada

### Repositório `comparador-consorcios-data`

Responsável por:

- coleta e preservação dos insumos;
- normalização e read models;
- contratos de dados;
- interpretação relativa auditável derivada dos mesmos dados validados;
- proveniência e freshness;
- manifesto, hashes e release fingerprint;
- publicação paralela, validação, state machine e rollback.

Não é responsável por:

- títulos e metas editoriais;
- canonical de páginas públicas;
- breadcrumbs/FAQ editoriais;
- sitemap e navegação do site;
- criação de páginas públicas a partir de JSON de rota.

### Frontend/site Sanida

Autoridade para:

- página PHP e sua intenção editorial;
- `config-site.php`;
- `head-global.php`;
- header/menu/footer e includes globais;
- canonical, title, description, H1, schema/FAQ quando aplicável;
- URLs efetivamente publicadas.

**Princípio:** centralizar mecanismo compartilhado nos includes do site; não centralizar conteúdo editorial em JSON de dados.

### Diretriz de frontend V2 — backend forte, frontend convencional

A arquitetura alvo adota deliberadamente o princípio **backend forte e auditável; frontend relativamente convencional e fácil de alterar**.

No backend, a complexidade é aceita quando protege uma invariável real: fonte, metodologia, contrato, proveniência, competência, integridade, interpretação relativa, manifesto, validação, publicação, state, rollback e ausência de fallback metodológico silencioso.

No frontend, a preferência é a arquitetura nativa já usada pelo site:

- PHP/HTML server-side para a estrutura e o conteúdo essencial da página;
- includes globais da Sanida para `head`, menu, footer e demais mecanismos compartilhados;
- CSS para apresentação;
- JavaScript para interação e progressive enhancement — busca, comparação, abertura de detalhes, filtros, URL/history e comportamento de interface;
- conteúdo essencial, contexto metodológico e informação indexável não devem depender de JavaScript para existir.

Um adaptador de dados como `_app/consorcio-data.php` pode permanecer quando trouxer ganho claro: localizar/resolver `current-v2`, ler os contratos V2 e entregar estruturas PHP coerentes à página. Esse adaptador **não** deve virar engine de UI, catálogo SEO, roteador editorial, reinterpretação metodológica nem mecanismo de fallback para V1.

A camada `interpretacao-relativa.v1` existe justamente para evitar que PHP/JS recalcularem mediana, quartil, posição por critério ou regras de missingness. O frontend deve consumir o contexto já versionado e limitar-se a apresentação, navegação e copy editorial compatível com o contrato.

`_app/consorcio-ui.php` é tratado como organização legada da V1, não como contrato arquitetural da V2. Ele **não deve ser portado mecanicamente** para a arquitetura final. Durante a migração pública, suas responsabilidades devem voltar para PHP/HTML convencional da página, CSS e JS; se a página ficar extensa, pequenos partials PHP podem ser usados apenas por legibilidade, sem criar uma nova camada de framework, estado ou contrato.

Regra de simplificação do frontend: **complexidade só permanece quando protege uma invariável importante**. Uma camada que apenas reorganiza a renderização, mas dificulta alterações editoriais ou de interface, deve ser eliminada ou reduzida.

## 4. Camadas de dados

1. `data/raw/` — insumos coletados;
2. `data/stage/` — normalizações intermediárias;
3. `data/runtime/` — estado efêmero de uma execução;
4. `data/source_state/` — memória durável C09 (`source-state.v1`);
5. `data/dist/` — read models **V1**, ainda ligados à produção pública;
6. `data/dist-v2/global/` — read models canônicos **V2 data-only**;
7. `hostgator/v2/` — publicação, validação, state machine e rollback V2.

`data/runtime/` e `data/source_state/` não são equivalentes: runtime descreve uma execução; source-state preserva entre runs a última consulta, último sucesso, última mudança, competência, hash do conteúdo e erro mais recente.

### Fronteira V1/V2 durante a migração

A V1 ainda usa `config/seo_routes.json` e `data/dist/seo/`. Eles **não devem ser removidos enquanto V1 estiver em produção**.

A V2 não usa `config/seo_routes.json` como insumo canônico e não publica JSON SEO. O workflow 12 #5 já removeu de `data/dist-v2` os antigos JSONs SEO residuais e publicou a primeira geração canônica data-only em `main`.

## 5. Fontes

### Fontes críticas da release V2

| Fonte | Papel | Conteúdo consumido | Competência |
|---|---|---|---|
| `bc_cadastro_admins` | cadastro atual | `data/stage/cadastro/instituicoes_cadastro.json` | não aplicável |
| `bc_filiais` | presença cadastral informativa | `data/stage/filiais/filiais.json` | data de posição quando disponível |
| `bc_consorciobd` | operação observada canônica | `data/raw/bc/consorciobd/latest_source.bin` | mês `YYYYMM` |
| `bc_ranking_reclamacoes` | reclamações oficiais | `data/raw/bc/ranking_reclamacoes/latest_source.csv` | semestre/período da fonte |

### Fontes auxiliares

- `bc_sgs` — contexto econômico; não integra os quatro estados críticos C09;
- `bc_consorciobd_trimestral` — complemento para enriquecimento/reconciliação; não é somado ao consolidado mensal;
- `abac_context` — contexto setorial; não substitui fonte oficial nem prova confiabilidade institucional.

A interpretação relativa não adiciona fontes. Ela é calculada **depois** de `core.validate_models`, exclusivamente a partir dos read models V2 já validados.

## 6. Workflows 01–07 — coleta

| # | Workflow | Agenda BRT | Papel |
|---|---|---|---|
| 01 | BC Cadastro | 05:27 diário + manual | catálogo atual + C09 |
| 02 | BC Filiais | 09:20 diário + manual | presença informativa + C09 |
| 03 | BC Séries | 05:50 diário + manual | séries SGS auxiliares |
| 04 | ConsorcioBD mensal | 06:37 dias 6,12,18,24,28 + manual | operação canônica + C09 |
| 05 | ConsorcioBD trimestral | 07:07 dias 7 e 21 + manual | enriquecimento/reconciliação |
| 06 | Ranking Reclamações | 06:43 dia 15 + manual | reclamações oficiais + C09 |
| 07 | ABAC | 07:47 segunda-feira + manual | contexto setorial |

Invariantes dos coletores:

- conteúdo só avança quando os bytes aprovados mudam;
- as quatro fontes críticas persistem `data/source_state/*.json` em toda tentativa;
- `changed:false` avança consulta/sucesso sem inventar `last_changed_at`;
- falha preserva último conteúdo/hash/competência aprovados e registra degradação;
- ordem física de linha do ranking BCB nunca vira posição oficial.

## 7. V1 ainda em produção — workflows 08 → 09 → 10

Este funil é legado, mas permanece operacional até o corte explícito.

- **08 · Build Read Models** — usa `transform/build_read_models.py`, configurações V1 e gera `data/dist/global/` + `data/dist/seo/`;
- **09 · HostGator Pull Ready** — valida readiness do contrato legado;
- **10 · Validate HostGator Pull** — valida o contrato V1 e, quando configurado, faz probe remoto.

`config/seo_routes.json` pertence a esse legado enquanto ele estiver ativo. A simplificação V2 não autoriza apagá-lo antecipadamente.

## 8. Builder e contratos V2 data-only

### Executável canônico

Workflows 11 e 12 devem chamar:

`transform/build_release_v2.py`

Ele usa `transform/build_read_models_v2.py` apenas como fachada de importação para as transformações e regras de domínio já testadas, aplica `transform/interpretation_v2.py` somente depois da validação dos modelos-base e **não consome configuração SEO nem produz artefatos SEO**.

Após a publicação canônica data-only, o antigo CLI foi endurecido para evitar regressão: `transform/build_read_models_v2.py` não pode mais ser executado diretamente e encerra com instrução para usar `build_release_v2.py`. A implementação histórica foi isolada em `transform/_read_models_v2_core_legacy.py` somente como núcleo interno temporário de transformação. Workflows 11/12 não executam esse módulo diretamente. Sua limpeza estrutural final pode ocorrer depois que o consumidor V2 estiver consolidado, sem reabrir metodologia nem misturar essa remoção com o corte público.

### Contratos de dados

- `instituicoes.v2` — identidade cadastral e presença informativa;
- `administradoras.v2` — perfil consolidado, cobertura e limites;
- `produtos.v2` — operação observada por raiz + competência + segmento;
- `rankings.v2` — reclamações sem posição fabricada;
- `segmentos.v2` — contexto por segmento;
- `comparacoes.v2` — comparação dimensional, sem ranking geral;
- `ofertas.v2` — camada comercial separada.

A interpretação é um subcontrato aditivo `interpretacao-relativa.v1` embutido em `administradoras.v2`, `segmentos.v2` e `comparacoes.v2`. Os contratos-base permanecem V2 porque nenhum campo existente é removido ou reinterpretado; consumidores antigos podem ignorar os campos novos. `meta.embedded_contracts.interpretacao_relativa` torna essa extensão explícita.

A release continua com sete JSONs globais não-meta mais `global/meta.json`. `meta.artifacts.seo=[]` permanece somente como envelope transitório de compatibilidade com a biblioteca operacional HostGator; **não há arquivo SEO físico, contrato SEO nem download SEO na release**.

`meta.release_scope` deve declarar:

- `kind=data_only`;
- `seo_artifacts=false`;
- `seo_owner=frontend_site`.

### Invariantes metodológicos

- `Segmentos_Consolidados` é a fonte canônica de estoques, fluxos e taxa por raiz + competência + segmento;
- grupos servem somente a enriquecimentos compatíveis e reconciliação;
- combinação raiz×segmento zerada não prova portfólio;
- um segmento só integra portfólio observado com sinal operacional positivo;
- zero é diferente de ausência;
- ausência é diferente de índice não divulgado;
- missingness não recebe nota neutra nem redistribuição de peso;
- presença/porte são informativos e não viram qualidade;
- ofertas comerciais não alteram avaliação institucional;
- `ranking_geral_publicavel=false`;
- não existem `scores` gerais na V2;
- ordenação por um critério não pode ser apresentada como ranking geral nem como posição oficial do BCB.

Na fixture auditada de maio/2026, 762 combinações consolidadas resultaram em 368 operações observadas em 124 raízes. Esses números são baseline de regressão, não constantes futuras.

### Interpretação relativa V1

A metodologia `2.1.0` acrescenta uma camada descritiva para apoiar a interface sem transferir cálculo metodológico ao frontend.

Por segmento e mesma competência, são calculados `min`, `q1`, `mediana`, `q3` e `max` para:

- taxa de administração observada;
- cotas ativas em dia;
- contemplações no mês;
- participação calculada de inadimplência.

Os quartis usam interpolação linear na posição `p*(n-1)`. Cada observação recebe comparação com a mediana, faixa da distribuição e ordem derivada do **critério isolado**, com `oficial=false`. Empates usam dense rank. Destaques textuais são emitidos somente nos quartis inferior/superior e permanecem descritivos.

O índice de reclamações BCB usa referência global entre administradoras do cadastro atual com índice divulgado; ele é institucional, não segmentado. Índice ausente/não divulgado não recebe zero, posição ou sinal favorável.

`leitura_confiabilidade` responde em termos de **suficiência de evidência para triagem**. Cadastro atual, operação observada, registro de reclamações e índice divulgado são sinais separados. A saída nunca equivale a “certificação de confiabilidade”.

Para contemplações, o contrato é explícito: o volume absoluto mensal pode ser comparado; **não se pode inferir probabilidade individual, tempo de contemplação nem declarar quem contempla mais rápido**.

Detalhamento para consumidores: `docs/INTERPRETATION_V2.md`.

## 9. C09 — atualidade e proveniência

Semântica:

- `generated_at` — geração dos read models;
- `last_checked_at` — última tentativa de consulta;
- `last_successful_check_at` — última consulta bem-sucedida;
- `last_changed_at` — última consulta em que o conteúdo persistido mudou;
- `competence` — período/data do conteúdo;
- `content_sha256` — hash dos bytes efetivamente consumidos;
- `last_check_status` / `last_error` — saúde da tentativa mais recente.

`transform/finalize_v2_release.py` reconcilia source-state com os bytes consumidos, injeta `source_status`/`freshness` no meta e cria `backend_release`.

No contrato data-only atual, `backend_release.contract = comparador-v2-release.v2` e o release fingerprint inclui também `release_scope`, para que a fronteira de responsabilidade da release faça parte de sua identidade.

### Evidência real histórica de C09 — 13/09/2026

As quatro fontes críticas foram executadas de verdade com `bootstrap=false`. Cadastro e filiais registraram mudança; ConsorcioBD e ranking retornaram `changed:false`, preservando `last_changed_at`. O workflow 12 consumiu esses estados e os reconciliou com os bytes usados no build.

## 10. Workflow 11 — validação V2

`.github/workflows/11-validate-v2.yml` prova a arquitetura sem publicar produção.

Fluxo atual:

1. Python 3.12 + PHP 8.2;
2. compilação do builder data-only, fachada/core, interpretação, finalizer e validadores;
3. testes unitários de contratos/C09/interpretação;
4. lint HostGator V2;
5. bootstrap de source-state da fixture quando necessário;
6. build data-only isolado em `/tmp`;
7. finalização de proveniência;
8. validação estrutural + baseline auditado + `config/deploy_v2.json`;
9. prova explícita de que não existe JSON SEO na release;
10. testes negativos PHP/HostGator;
11. upload do preview.

O baseline auditado cobre, entre outros: 130 administradoras, 85 registros de reclamações, 368 operações/124 raízes, distribuição por segmento, totais de imóveis, portfólios de controle, órfão operacional e ausência de posição oficial fabricada. Os testes de interpretação adicionam quartis, dense rank, missingness de reclamações, leitura de confiabilidade sem certificação e proibição de inferência de contemplação.

## 11. Workflow 12 — geração canônica V2

`.github/workflows/12-build-publish-v2.yml` roda manualmente e às 10:00 BRT.

Fluxo:

1. captura commit de entrada;
2. compila e testa backend V2, incluindo interpretação relativa;
3. bootstrap apenas de source-state ausente;
4. constrói `data/dist-v2` via `build_release_v2.py`;
5. remove qualquer `data/dist-v2/seo/*.json` residual;
6. finaliza proveniência;
7. valida release data-only e gate HostGator;
8. faz upload do candidato contendo somente globais V2 + source-state/runtime;
9. recusa publicar se a branch avançou durante o build;
10. usa `git add -A data/dist-v2 data/source_state`, registrando também a remoção de artefatos V2 antigos;
11. publica somente se houver diff real.

**Regra de stale base:** nunca rebasear artefato pronto sobre inputs mais novos. Se outro writer avançar a branch, iniciar uma nova execução do workflow 12 sobre o novo HEAD. Não usar “Re-run failed jobs” para contornar esse gate.

Após disparos manuais dos coletores, aguardar writers derivados, especialmente workflow 08, antes de iniciar manualmente o 12.

### Primeira publicação canônica data-only — 13/09/2026

Workflow 12 #5:

- input SHA: `04bc5a226f932d43e19bc27990f07f36b903af4c`;
- conclusão: `success`;
- pipeline: `4.1.0`;
- contrato: `comparador-v2-release.v2`;
- `release_scope=data_only`;
- sete artefatos de dados não-meta;
- `degraded_sources=[]`;
- `source_fingerprint=72456642883fa6cc56f9169d2572394e899ad3cec2d86816f7e22e63f8681780`;
- `release_fingerprint=9cd474256d02174d0033fb2e1398499c3d26fa9dc130db4ed584339b54f41cb8`;
- commit canônico publicado: `5664bd5360671ad94485aa8b11ab90fc142e2732`;
- `data/dist-v2/seo/` ausente.

A extensão `4.2.0` não substitui esse registro histórico: sua primeira publicação canônica deve ser documentada depois de uma execução válida do workflow 12.

## 12. HostGator V2

Arquivos em `hostgator/v2/`:

- `consorcio-v2-lib.php` — manifesto, integridade, lock, state e primitivas;
- `consorcio-v2-release-gate.php` — gate de release/proveniência;
- `consorcio-pull-deploy-v2.php` — fetch, staging, validação, promoção e quarentena;
- `consorcio-validate-current-v2.php` — valida `current-v2`;
- `consorcio-rollback-v2.php` — rollback somente para release válida;
- `consorcio-v2-config.php` — origem, paths, contratos e política.

No contrato atual, `consorcio-v2-config.php` é `2.3.0`, exige somente os sete contratos globais não-meta e `comparador-v2-release.v2`. A interpretação relativa não altera essa lista porque é subcontrato embutido nos mesmos JSONs.

A cadeia operacional permanece:

`main` → SHA imutável → `global/meta.json` → arquivos declarados → SHA/tamanho/JSON/contratos/source_status → staging → validação → `releases-v2/<id>` → swap atômico de `current-v2` → revalidação → state.

Pull, validate e rollback compartilham lock e gate. Tentativa de publicação e validação da release ativa têm states separados. Candidata rejeitada não substitui release saudável.

## 13. Homologação HostGator histórica — contrato v1

Em 13/09/2026 foi homologado no HostGator real o mecanismo operacional sob `comparador-v2-release.v1`:

- base paralela `/home1/sanid210/comparador-consorcios-v2-homolog`;
- PHP CLI 8.2.33;
- commit de origem `34b5f53e35499352086dc6dc94f9c8cc944507d3`;
- release `20260913T154448Z_34b5f53e_2009151a`;
- `manifest_sha256=2009151aca2a6655027eadf6b38eb279d169b13ae6a20149a67a37e8c75eae78`;
- `source_fingerprint=89b47d7af9159b3aa7bebaa8ea7eb93ff7ad1e91ebc6831e9195dd777492a996`;
- `release_fingerprint=db7a2470c8edb517ceea14a9e2a343d71f5eae2e1e508401deaf6fd194ccfb68`;
- 10 artefatos não-meta validados naquela versão (7 dados + 3 SEO então existentes);
- `degraded_sources=[]`;
- dry-run, publicação, validação independente e `no_change/RC=10` aprovados;
- state/logs inspecionados; ausência de quarentena coerente com nenhuma candidata rejeitada;
- rollback ensaiado com segunda release local de bytes idênticos, seguido de revalidação;
- V1 permaneceu intacta em `/home1/sanid210/consorcio-data/current`.

Essa evidência continua provando lock, staging, promoção, symlink, state, rollback e isolamento V1/V2.

## 14. C15/C16 — inventário, SEO e frontend convencional

Snapshot read-only do HostGator em 13/09/2026 confirmou 13 rotas reais do cluster:

- `/financas/consorcio/`;
- `/carro/`;
- `/imobiliario/`;
- `/moto/`;
- `/carta-contemplada/`;
- `/servicos/`;
- `/servicos/cirurgia-plastica/`;
- `/eletroeletronicos/`;
- `/moveis-planejados/`;
- `/moto/eletrica/`;
- `/moto/usada/`;
- `/moto/bradesco/`;
- `/moto/porto-seguro/`.

Também confirmou que quatro rotas declaradas no antigo `seo_routes.json` não têm PHP correspondente no host:

- `melhor-consorcio.php`;
- `consorcio-ou-financiamento.php`;
- `ranking-administradoras.php`;
- `menor-taxa.php`.

Essas páginas **não devem ser criadas apenas porque existia configuração JSON**. Em especial, a intenção “consórcio ou financiamento” já possui território editorial próprio no blog e não deve ganhar uma segunda página técnica por efeito colateral do pipeline.

O hub `/financas/consorcio/` é o consumidor central da ferramenta. As páginas filhas usam os includes normais do site e não precisam carregar o motor do comparador.

A migração pública deve, portanto:

- retirar a dependência estrutural de `_app/consorcio-seo.php`;
- deixar SEO da página no próprio PHP + `head-global.php`;
- preservar `config-site.php` como autoridade de sitemap/navegação;
- migrar apenas o consumidor de dados para `current-v2`;
- não fabricar novas URLs, redirects ou `noindex` por existência histórica de um JSON;
- não portar `_app/consorcio-ui.php` como camada obrigatória da V2;
- manter, se útil, apenas um adaptador de dados pequeno e explícito entre `current-v2` e a página;
- renderizar o conteúdo essencial em PHP/HTML server-side e usar CSS/JS diretamente para apresentação e interação;
- admitir partials PHP somente por legibilidade, sem transformar partials em engine de UI ou novo sistema de contratos;
- consumir `interpretacao-relativa.v1` sem recalcular no navegador/PHP os benchmarks metodológicos canônicos.

Essa decisão é uma **diretriz para a migração**, não uma descrição da produção atual: `consorcio-seo.php` e `consorcio-ui.php` ainda podem existir enquanto V1 permanecer pública e não devem ser removidos antes do corte controlado.

## 15. Estado de ativação

### Marco A — backend/metodologia V2

**Base data-only concluída no GitHub.** Metodologia, read models, C09 e gates centrais foram implementados e auditados; workflow 12 #5 publicou a release canônica `comparador-v2-release.v2`/pipeline `4.1.0` no commit `5664bd5360671ad94485aa8b11ab90fc142e2732`.

A extensão de interpretação `2.1.0`/`4.2.0` deve cumprir o mesmo gate: testes verdes, merge e nova geração canônica. Ela não reabre score geral e não altera o número de artefatos da release.

### Marco B — publicação operacional

O mecanismo de publicação é separado da metodologia. Uma release com interpretação relativa continua sujeita aos mesmos hashes, manifesto, staging, validação, lock, promoção e rollback; o subcontrato embutido não autoriza atalhos no HostGator.

### Marco C — V2 pública

Ainda pendente. Exige migração/homologação do frontend, principalmente:

- **C06** — remover fallback metodológico V1;
- **C07** — labels, unidades, datas e formatters V2;
- **C09** — apresentar competência/atualidade corretamente;
- **C14** — cobertura, período, motivos e limites junto da comparação;
- **C15** — cortar a dependência SEO do pipeline de dados e alinhar ao padrão nativo do site;
- **C16** — migrar para frontend convencional PHP/HTML + CSS/JS, sem engine estrutural de UI; homologar teclado, foco, 390 px, zoom 200%, sem-JS e histórico/estado da consulta;
- **interpretação** — usar o subcontrato versionado para pistas visuais/textuais, sem criar score/ranking geral no frontend.

Enquanto Marco C estiver pendente, V1 continua pública e `deploy_enabled=false`.

## 16. Checklist contra redução silenciosa

Antes de apagar, fundir ou simplificar componente, verificar:

- é V1 ainda em produção ou V2 paralela?
- participa de coleta, source-state, manifesto, validação ou rollback?
- muda a relação conteúdo ↔ `content_sha256`?
- perde consulta/sucesso/mudança/competência?
- permite JSON físico não declarado?
- enfraquece hash/tamanho/contrato?
- muda as quatro fontes críticas?
- confunde publicação com validação?
- remove quarentena, lock, staging, swap ou rollback?
- reintroduz score, posição fabricada, dupla contagem ou presença como qualidade?
- altera a semântica `Segmentos_Consolidados` versus grupos?
- mistura SEO editorial com a release de dados novamente?
- recalcula no frontend mediana, quartis, ordens por critério ou missingness que já pertencem ao subcontrato de interpretação?
- transforma contemplações absolutas em promessa de rapidez/probabilidade?
- cria uma engine de UI, estado ou roteamento sem proteger uma invariável que justifique essa complexidade?

Se sim, a mudança é arquitetural e precisa de justificativa, teste e atualização deste memorial.

## 17. Princípio final

Uma PR verde prova código/fixtures. Uma geração canônica prova o funil. Uma release validada no HostGator prova publicação operacional. Nenhuma dessas etapas, isoladamente, prova que o frontend público consome corretamente a V2.

A cadeia final deve permanecer auditável:

**fonte → tentativa de coleta → estado durável → bytes consumidos → read models validados → interpretação relativa versionada → builder data-only → manifesto → commit → release → validação HostGator → adaptador de dados PHP → HTML server-side + CSS/JS do site.**

A disciplina final é deliberada: **backend forte e auditável; frontend convencional, legível e fácil de alterar**.
