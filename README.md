# Comparador de Consórcios Data

Pipeline canônico de dados do **Comparador de Consórcios Sanida**.

> **Papel deste README**
>
> Este arquivo é o **memorial técnico e operacional do projeto**. Ele deve permitir que outra conversa, outro operador ou outro momento de manutenção compreenda os funis, contratos, invariantes, estados e fronteiras de produção sem depender do histórico de uma sessão anterior.
>
> O código e os arquivos de configuração continuam sendo a verdade executável. O README é a memória arquitetural que explica **por que** cada peça existe e o que **não pode ser removido ou simplificado silenciosamente**.

## 1. Regra de preservação arquitetural

Qualquer alteração material em coletores, workflows, contratos, papéis das fontes, proveniência, publicação, rollback, caminhos de artefatos ou fronteiras V1/V2 deve atualizar este README **na mesma PR**.

Regras de mudança:

- ausência de uma peça neste README **não autoriza sua remoção automática**; antes de reduzir código, confirmar se ela participa de algum funil, fallback, gate, auditoria ou etapa de migração;
- não substituir um funil por outro apenas porque parecem produzir arquivos semelhantes;
- não remover V1 enquanto a migração V2 não tiver encerramento explícito e comprovado em produção;
- não confundir `generated_at`, consulta da fonte, mudança do conteúdo, competência do dado e publicação da release;
- não transformar ausência de dado em zero, nota neutra ou redistribuição de peso;
- não reintroduzir score/ranking geral sem nova metodologia versionada e nova auditoria;
- não alterar `current-v2`, manifesto, validador ou state de forma independente: eles formam um único contrato operacional;
- uma otimização de código só é aceitável se preservar os contratos, gates, estados de falha e capacidade de auditoria descritos aqui.

## 2. Estado atual do projeto

O `main` ainda alimenta a versão publicada existente. A reforma **V2** foi homologada no backend e na camada operacional paralela do HostGator, mas **ainda não foi ativada para o usuário final**. A V1 continua sendo a produção pública enquanto o consumidor PHP/frontend não for migrado e homologado.

A V2 foi desenhada para responder:

1. **Quem é a administradora pesquisada?**
2. **Em quais segmentos existe operação observada?**
3. **Quais sinais públicos existem para avaliar sua operação e reclamações — e quais são os limites desses sinais?**
4. **Como ela se compara a outras administradoras em dimensões equivalentes?**

A V2 inicial deliberadamente **não possui nota ou ranking geral**.

Documentos normativos e operacionais:

- [`docs/METODOLOGIA_V2.md`](docs/METODOLOGIA_V2.md) — contrato metodológico humano;
- [`config/methodology_v2.json`](config/methodology_v2.json) — política metodológica executável;
- [`config/provenance_v2.json`](config/provenance_v2.json) — contrato de consulta, mudança, hash e competência das fontes críticas;
- [`config/audit_baseline_v2.json`](config/audit_baseline_v2.json) — fotografia da Auditoria 2 usada somente para regressão da fixture;
- [`config/deploy_v2.json`](config/deploy_v2.json) — contrato de publicação V2, ainda com `deploy_enabled=false` enquanto o consumidor público não migra;
- [`docs/REMEDIATION_STATUS_V2.md`](docs/REMEDIATION_STATUS_V2.md) — mapa C01–C16 e situação da homologação real.

## 3. Mapa das camadas de dados

O repositório separa conteúdo, normalização, estado e publicação:

1. `data/raw/` — cópia dos insumos coletados;
2. `data/stage/` — normalizações dos coletores quando existe etapa intermediária;
3. `data/runtime/` — estado efêmero de uma execução local/Action; **não é memória durável entre runs**;
4. `data/source_state/` — memória durável C09 das fontes críticas (`source-state.v1`);
5. `data/dist/` — read models da geração **V1**, ainda ligados à produção atual;
6. `data/dist-v2/` — read models canônicos do backend **V2**;
7. `hostgator/v2/` — publicação, validação, state machine e rollback da release V2 no host.

### Regra importante

`data/runtime/` e `data/source_state/` têm funções diferentes. O primeiro descreve uma execução. O segundo preserva, entre execuções e commits, a história mínima necessária para afirmar quando uma fonte foi consultada, quando teve sucesso, quando mudou e a que competência se refere.

## 4. Inventário das fontes

As fontes configuradas em `config/sources.json` têm papéis distintos.

### Fontes críticas para a release V2

São obrigatórias em `config/provenance_v2.json` e precisam aparecer exatamente em `meta.source_status`:

| Fonte | Papel V2 | Conteúdo consumido | Competência |
|---|---|---|---|
| `bc_cadastro_admins` | cadastro atual das administradoras | `data/stage/cadastro/instituicoes_cadastro.json` | não aplicável |
| `bc_filiais` | presença cadastral **informativa** | `data/stage/filiais/filiais.json` | data de posição quando disponível |
| `bc_consorciobd` | operação observada canônica | `data/raw/bc/consorciobd/latest_source.bin` | mês `YYYYMM` |
| `bc_ranking_reclamacoes` | reclamações oficiais | `data/raw/bc/ranking_reclamacoes/latest_source.csv` | semestre/período informado pela fonte |

### Fontes auxiliares

- `bc_sgs` — séries SGS utilizadas como contexto/indicadores econômicos; não integra o conjunto de quatro estados críticos C09 da release V2;
- `bc_consorciobd_trimestral` — base trimestral complementar, separada do consolidado mensal canônico usado nos read models V2 atuais;
- `abac_context` — contexto setorial complementar; não substitui fonte oficial nem entra como prova de confiabilidade institucional.

## 5. Funis de coleta — workflows 01 a 07

Os sete primeiros workflows são coletores. Eles não são intercambiáveis.

### 01 · Coleta BC Cadastro

Arquivo: `.github/workflows/01-coleta-bc-cadastro.yml`

- agenda: 05:27 BRT (`27 8 * * *`) + execução manual;
- fonte: Banco Central / Olinda / `SedesConsorcios`;
- coletor: `collectors/bc_cadastro_admins.py`;
- modo homologado: `odata-csv`;
- possui retry próprio;
- produz raw, stage e runtime;
- só versiona raw/stage quando o conteúdo efetivamente muda;
- **sempre** converte a tentativa em `data/source_state/bc_cadastro_admins.json`, inclusive em `changed:false` ou falha;
- competência: `not_applicable`.

Este é o catálogo atual usado para definir o universo de administradoras da V2.

### 02 · Coleta BC Filiais

Arquivo: `.github/workflows/02-coleta-bc-filiais.yml`

- agenda: 09:20 BRT (`20 12 * * *`) + manual;
- fonte: Banco Central / Olinda / filiais de administradoras;
- coletor: `collectors/bc_filiais.py`;
- modo homologado: `odata-csv`;
- produz raw, stage e runtime;
- persiste `data/source_state/bc_filiais.json` em toda tentativa;
- competência inferida da data de posição quando a fonte a fornece.

**Invariante metodológico:** filiais/presença são informativas. Número de filiais não é proxy de qualidade, confiabilidade ou disponibilidade comercial nacional.

### 03 · Coleta BC Séries

Arquivo: `.github/workflows/03-coleta-bc-series.yml`

- agenda: 05:50 BRT (`50 8 * * *`) + manual;
- coletor: `collectors/bc_sgs.py`;
- usa `config/series_map.json`;
- modo homologado: `sgs-json`;
- versiona dados em `data/raw/bc/sgs/` somente quando mudam.

A SGS 25497 está documentada como `% a.m.` e com seu recorte específico. Qualquer anualização deve ser um indicador derivado separado.

### 04 · Coleta ConsorcioBD mensal

Arquivo: `.github/workflows/04-coleta-consorciobd.yml`

- agenda: 06:37 BRT nos dias 6, 12, 18, 24 e 28 (`37 9 6,12,18,24,28 * *`) + manual;
- coletor: `collectors/bc_consorciobd.py`;
- modo homologado: `direct-content-download`;
- pesquisa a competência disponível e baixa o ZIP oficial;
- persiste raw e runtime;
- persiste `data/source_state/bc_consorciobd.json` em toda tentativa;
- conteúdo efetivamente vinculado ao estado: `latest_source.bin`;
- competência: mês oficial encontrado.

Esta é a **fonte operacional canônica** da V2 atual.

### 05 · Coleta ConsorcioBD trimestral

Arquivo: `.github/workflows/05-coleta-consorciobd-trimestral.yml`

- agenda: 07:07 BRT nos dias 7 e 21 (`07 10 7,21 * *`) + manual;
- coletor: `collectors/bc_consorciobd_trimestral.py`;
- modo homologado: `direct-content-download`;
- produz raw/runtime próprios.

É uma fonte complementar. Não deve ser somada ou confundida com o consolidado mensal usado pelo builder V2.

### 06 · Coleta Ranking Reclamações

Arquivo: `.github/workflows/06-coleta-ranking-reclamacoes.yml`

- agenda: 06:43 BRT no dia 15 de cada mês (`43 9 15 * *`) + manual;
- coletor: `collectors/bc_ranking_reclamacoes.py`;
- modo homologado: `direct-content-download`;
- produz `latest.json`, `latest_source.csv` e runtime;
- persiste `data/source_state/bc_ranking_reclamacoes.json` em toda tentativa;
- competência inferida do período oficial selecionado.

**Invariante:** ordem física de linha nunca vira posição oficial. `posicao_oficial` só pode existir se a fonte trouxer coluna explícita correspondente.

### 07 · Coleta ABAC

Arquivo: `.github/workflows/07-coleta-abac.yml`

- agenda: segunda-feira, 07:47 BRT (`47 10 * * 1`) + manual;
- coletor: `collectors/abac_context.py`;
- modo homologado: `context-probe`;
- produz contexto em `data/raw/abac/latest.json`.

ABAC é contexto complementar; não substitui cadastro, operação ou reclamações oficiais.

## 6. Funil V1 ainda em produção — workflows 08 → 09 → 10

Este funil continua existindo **por decisão de migração segura**. Ele não deve ser apagado só porque V2 já foi homologada operacionalmente.

### 08 · Build Read Models

Arquivo: `.github/workflows/08-build-read-models.yml`

- roda manualmente, por agenda diária e após sucesso dos coletores 01–07;
- usa o builder legado `transform/build_read_models.py`;
- usa `config/scoring_rules.json`, `config/product_rules.json` e rotas SEO legadas;
- gera `data/dist/global/` e `data/dist/seo/`;
- versiona outputs quando existe diff.

É o builder ligado à versão atualmente publicada.

### 09 · HostGator Pull Ready

Arquivo: `.github/workflows/09-deploy-hostgator.yml`

- roda manualmente ou após o workflow 08;
- valida o contrato de pull legado com `config/deploy.json` e `data/dist/global/meta.json`;
- produz artifact de readiness.

### 10 · Validate HostGator Pull

Arquivo: `.github/workflows/10-validate-hostgator.yml`

- roda manualmente ou após o workflow 09;
- valida o contrato local legado;
- quando `HOSTGATOR_PULL_PUBLIC_BASE_URL` está configurado, também executa probe remoto da publicação atual.

**Fronteira:** 08–10 são V1/produção atual. Não são a publicação HostGator V2.

## 7. Metodologia e read models V2

Builder: `transform/build_read_models_v2.py`.

No funil canônico, o `dist_base_dir` é sobrescrito para `data/dist-v2`.

Contratos produzidos:

- `instituicoes.v2` — identidade cadastral atual e presença informativa;
- `administradoras.v2` — perfil consolidado, cobertura e limites da leitura;
- `produtos.v2` — operação observada por raiz + competência + segmento;
- `rankings.v2` — reclamações sem fabricar posição;
- `segmentos.v2` — contexto por segmento;
- `comparacoes.v2` — comparação dimensional, sem ranking geral;
- `ofertas.v2` — contrato comercial separado.

### Invariantes metodológicos que não podem ser simplificados

- `Segmentos_Consolidados` é a fonte de estoques, fluxos e taxa por raiz + competência + segmento;
- arquivos de grupos servem apenas a enriquecimentos compatíveis, como prazo/valor médio e reconciliação; **não são somados ao consolidado**;
- combinação raiz×segmento zerada não prova portfólio;
- um segmento só entra no portfólio observado se houver sinal operacional positivo;
- zero é diferente de ausência;
- ausência é diferente de índice não divulgado;
- missingness não recebe nota neutra;
- pesos não são redistribuídos pela ausência de uma dimensão;
- presença/porte não se convertem em qualidade;
- ofertas comerciais ficam separadas das evidências institucionais;
- `ranking_geral_publicavel=false` na V2 inicial;
- não existem `scores` gerais na V2 inicial;
- a V2 organiza evidências para triagem; não certifica solvência, contemplação ou adequação individual.

Na fotografia auditada de maio/2026, 762 combinações consolidadas resultaram em **368 operações observadas em 124 raízes**. Esses números pertencem à fixture de regressão, não são uma constante futura do mercado.

## 8. C09 — atualidade, proveniência e estado durável

C09 existe para impedir que uma nova geração do site pareça significar que todas as fontes foram atualizadas naquele momento.

### Semântica dos campos

- `generated_at` — momento em que os read models foram gerados;
- `last_checked_at` — última tentativa registrada de consulta à fonte;
- `last_successful_check_at` — última consulta concluída com sucesso, mesmo que sem mudança;
- `last_changed_at` — última consulta em que os bytes persistidos mudaram;
- `competence` — período/data a que o conteúdo se refere;
- `content_sha256` — hash dos **bytes efetivamente consumidos** pelo builder;
- `last_check_status` / `last_error` — saúde da tentativa mais recente.

### Regras de falha

Uma consulta `changed:false` avança consulta/sucesso, mas não avança artificialmente `last_changed_at`.

Uma tentativa com erro:

- preserva o último conteúdo aprovado;
- preserva o último hash e competência aprovados;
- registra degradação explícita;
- não transforma automaticamente a última release válida em inválida.

Uma release só é elegível se o estado durável puder ser reconciliado com os bytes reais que o builder consumiu. Estado bonito, mas desconectado do arquivo, é erro bloqueante.

`transform/finalize_v2_release.py` incorpora os estados em `global/meta.json` sob `source_status` e `freshness` e cria o contrato de release `comparador-v2-release.v1`.

### Evidência real de C09 — 13/09/2026

As quatro fontes críticas foram executadas no GitHub Actions com `bootstrap=false`. Cadastro e filiais registraram mudança real; ConsorcioBD mensal e ranking de reclamações retornaram `changed:false`, avançando `last_checked_at`/`last_successful_check_at` sem avançar artificialmente `last_changed_at`. Depois disso, o workflow 12 publicou a release canônica usando esses estados persistidos. Isso comprova a semântica C09 fora de fixture.

## 9. Funil de validação V2 — workflow 11

Arquivo: `.github/workflows/11-validate-v2.yml`.

Objetivo: provar em PR que a arquitetura V2 continua íntegra sem publicar produção.

Executa:

1. Python 3.12 e PHP 8.2;
2. compilação dos scripts V2;
3. testes unitários de contratos e C09;
4. lint PHP HostGator V2;
5. bootstrap de source-state em fixture quando necessário;
6. build isolado em `/tmp`;
7. finalização de proveniência;
8. validação estrutural + baseline auditado;
9. testes negativos do HostGator V2;
10. empacotamento e upload de preview.

O baseline de `config/audit_baseline_v2.json` verifica a fotografia da Auditoria 2, incluindo 130 administradoras, 85 linhas de reclamações, 368 operações em 124 raízes, distribuição de segmentos, totais de imóveis e casos de controle. **Esses números não são usados para bloquear mudanças legítimas no workflow recorrente 12.**

## 10. Funil de geração canônica V2 — workflow 12

Arquivo: `.github/workflows/12-build-publish-v2.yml`.

Agenda prevista: 10:00 BRT (`0 13 * * *`) + execução manual.

Fluxo:

1. checkout completo e captura do commit de entrada;
2. Python 3.12 + PHP 8.2;
3. testes unitários;
4. bootstrap apenas dos source-states ainda ausentes durante a migração inicial;
5. configuração temporária apontando o builder para `data/dist-v2`;
6. build V2;
7. anexação de proveniência;
8. validação estrutural recorrente — sem congelar contagens históricas;
9. validação PHP/HostGator V2;
10. upload de `comparador-v2-release-candidate`;
11. fetch explícito da branch remota e comparação com o commit de entrada;
12. se a branch avançou durante o build, a publicação é recusada;
13. se a base continua idêntica, versiona `data/dist-v2` + `data/source_state` no commit de publicação.

**Invariante de reprodutibilidade:** uma release já construída nunca é rebaseada sobre novos inputs. Se o branch mudar durante o build, o ciclo falha e uma nova geração deve nascer do novo commit.

### Evidência real e regra operacional de concorrência — 13/09/2026

- a primeira publicação canônica V2 real foi concluída com sucesso;
- em uma execução posterior, o workflow 12 chegou até o candidato válido, mas recusou publicar porque o workflow 08 havia avançado `main` durante o build; o gate de stale base funcionou como desenhado;
- nesses casos, **não usar “Re-run failed jobs”**: iniciar um novo workflow 12 a partir do `main` já estabilizado;
- após execuções manuais de coletores, aguardar workflows escritores derivados — especialmente o 08 — terminarem antes de disparar manualmente o 12;
- uma execução seguinte, já com a branch estabilizada, publicou normalmente;
- uma nova execução do workflow 12 sobre a mesma base gerou artefatos idênticos e terminou com `V2 já está canônica; nenhum commit necessário.`, comprovando comportamento determinístico e ausência de commit artificial.

O gate de stale base **não deve ser removido ou enfraquecido**. Se colisões se tornarem recorrentes a ponto de prejudicar a operação, a correção deve ser serialização coordenada dos writers, não rebase de artefato já construído.

## 11. Publicação operacional HostGator V2

A camada paralela está em `hostgator/v2/`:

- `consorcio-v2-lib.php` — manifesto, integridade, lock, state e primitivas comuns;
- `consorcio-v2-release-gate.php` — contrato de backend/proveniência obrigatório;
- `consorcio-pull-deploy-v2.php` — resolução de commit, download, staging, validação, promoção, swap e quarentena;
- `consorcio-validate-current-v2.php` — validação do `current-v2`;
- `consorcio-rollback-v2.php` — rollback somente para release que passe no mesmo gate;
- `consorcio-v2-config.php` — origem, paths, contratos obrigatórios e política de validação.

### Caminho de uma publicação V2

`main` → resolver SHA imutável → baixar `data/dist-v2/global/meta.json` → ler manifesto → baixar somente arquivos declarados daquele mesmo SHA → conferir tamanho/SHA/JSON/contratos/source_status → staging → validar staging → promover para `releases-v2/<release-id>` → trocar atomicamente `current-v2` → validar novamente o target ativo → gravar state de sucesso.

### Estado de validação x estado de publicação

São conceitos diferentes:

- `last_validation_attempt` e `last_validation_success` descrevem a **saúde da release validada**;
- `last_publication_attempt` e `last_publication_success` descrevem a **tentativa de buscar/promover uma candidata**.

Falha de rede, manifesto candidato inválido ou erro de download não pode “envenenar” o histórico de validação da release ativa saudável.

### Quarentena e rollback

- manifesto rejeitado entra em quarentena;
- `--force` só deve ser usado conscientemente; na homologação de 13/09/2026 ele foi usado exclusivamente para produzir uma segunda release local de bytes idênticos e testar o mecanismo operacional de rollback;
- se houver falha pós-swap, o processo tenta restaurar a release anterior;
- rollback manual também valida a release alvo antes e depois do swap;
- pull, validate e rollback compartilham o mesmo lock e o mesmo gate.

`current-v2` é separado da publicação V1 e deve permanecer assim até o corte explícito do consumidor.

### Homologação real no HostGator — 13/09/2026

Base paralela instalada:

`/home1/sanid210/comparador-consorcios-v2-homolog`

Evidências registradas:

- PHP CLI real: `8.2.33`;
- scripts de `hostgator/v2` instalados em `bin-v2/` e aprovados no `php -l`;
- dry-run concluído antes da criação de `current-v2`;
- commit canônico de origem: `34b5f53e35499352086dc6dc94f9c8cc944507d3`;
- release homologada: `20260913T154448Z_34b5f53e_2009151a`;
- `manifest_sha256`: `2009151aca2a6655027eadf6b38eb279d169b13ae6a20149a67a37e8c75eae78`;
- `source_fingerprint`: `89b47d7af9159b3aa7bebaa8ea7eb93ff7ad1e91ebc6831e9195dd777492a996`;
- `release_fingerprint`: `db7a2470c8edb517ceea14a9e2a343d71f5eae2e1e508401deaf6fd194ccfb68`;
- 10 artefatos validados pelo mesmo gate de backend/proveniência;
- `degraded_sources=[]`;
- segunda chamada sem mudança retornou `RC=10`, `result=no_change` e `reason=remote_manifest_unchanged_current_healthy`;
- `last_publication_*`, `last_validation_*`, `current_release.json` e logs foram verificados no host real;
- ausência de `rejected_releases.json` foi confirmada, coerente com nenhuma candidata rejeitada durante a homologação;
- o ensaio de rollback criou uma segunda release local por `--force`, com bytes/manifesto/fingerprint idênticos, promoveu-a, voltou explicitamente à primeira release e revalidou `current-v2` com sucesso;
- esse ensaio comprova o mecanismo de staging, promoção, symlink swap, gate, rollback e revalidação, **não** uma reversão entre dois conteúdos de dados distintos;
- a V1 permaneceu intacta em `/home1/sanid210/consorcio-data/current`, apontando para `/home1/sanid210/consorcio-data/releases/20260913T152026Z_d6066ac` ao fim do ensaio.

A pasta de homologação e as duas releases V2 locais devem ser preservadas enquanto servirem de baseline para a migração do consumidor.

## 12. Inventário resumido dos 12 workflows

| # | Workflow | Papel | Estado de migração |
|---|---|---|---|
| 01 | BC Cadastro | coleta catálogo atual + C09 | compartilhado, homologado para V2 |
| 02 | BC Filiais | presença informativa + C09 | compartilhado, homologado para V2 |
| 03 | BC Séries | séries SGS | auxiliar |
| 04 | ConsorcioBD mensal | operação canônica + C09 | crítico V2, estado real validado |
| 05 | ConsorcioBD trimestral | complementar | auxiliar |
| 06 | Ranking Reclamações | reclamações oficiais + C09 | crítico V2, estado real validado |
| 07 | ABAC | contexto setorial | auxiliar |
| 08 | Build Read Models | builder legado | V1 em produção |
| 09 | HostGator Pull Ready | readiness legado | V1 em produção |
| 10 | Validate HostGator Pull | validação/probe legado | V1 em produção |
| 11 | Validate Comparador V2 | CI/regressão V2 | backend V2 homologado em código |
| 12 | Build & Publish Comparador V2 | geração canônica `dist-v2` | operacionalmente homologado |

## 13. Estado de ativação e definição de “pronto”

Existem três marcos distintos:

### A. Backend V2 pronto em código — **CONCLUÍDO**

Exige workflow 11 verde no HEAD correspondente, incluindo Python, PHP 8.2, C09, manifesto, contratos e testes negativos.

### B. Backend V2 homologado operacionalmente — **CONCLUÍDO em 13/09/2026**

Além de A, o checklist exigia:

1. executar o workflow 12 como workflow real e obter release candidate + commit canônico coerentes — **concluído**;
2. confirmar `data/source_state` das quatro fontes críticas em execução real, não apenas fixture — **concluído**;
3. instalar `hostgator/v2` em paralelo no HostGator real — **concluído**;
4. confirmar PHP 8.2 no host — **concluído com PHP 8.2.33**;
5. executar pull/dry-run conforme aplicável — **concluído**;
6. publicar uma release V2 concreta em `releases-v2` — **concluído**;
7. validar `current-v2` com o mesmo manifesto/gate — **concluído**;
8. verificar `last_publication_*`, `last_validation_*`, quarentena e logs — **concluído**;
9. ensaiar rollback para uma release anterior válida — **concluído para o mecanismo operacional, usando duas releases locais de bytes idênticos**;
10. confirmar que V1 permanece intacta durante o ensaio — **concluído**.

**Conclusão do Marco B:** o backend V2 e sua camada de publicação operacional estão homologados em paralelo. Isso não significa que a V2 já esteja pública.

### C. V2 em produção para o usuário — **PENDENTE**

Além de B, exige migração e homologação do consumidor PHP/frontend, incluindo C06, C07, C14, C15 e C16. Só então deve ser considerada qualquer mudança de `deploy_enabled`, paths públicos ou desativação da V1.

Enquanto C estiver pendente:

- `config/deploy_v2.json` deve permanecer com `deploy_enabled=false`;
- V1 continua sendo a produção pública;
- `current-v2` continua sendo uma instalação paralela de homologação, não o contrato de consumo público.

## 14. Pendências que pertencem ao consumidor/frontend

O backend não deve tentar “resolver” estas pendências reintroduzindo campos legados:

- **C06** — consumidor deve abandonar fallback silencioso para score/metodologia V1;
- **C07** — labels, percentuais, datas e unidades devem refletir os contratos V2;
- **C09** — frontend deve apresentar corretamente competência/atualidade; o backend já transporta e teve essa semântica comprovada em execução real;
- **C14** — mostrar cobertura, período, motivos e limites próximos da comparação;
- **C15** — reconciliar consumidores e rotas SEO reais antes do corte;
- **C16** — teclado, foco, 390 px, zoom 200%, sem-JS e histórico/estado da consulta.

A próxima etapa recomendada é iniciar por **C15**, inventariando consumidores PHP e rotas SEO efetivamente publicadas para delimitar o corte sem criar fallback ou destino órfão.

## 15. Checklist contra redução silenciosa

Antes de apagar, fundir ou simplificar qualquer componente, verificar explicitamente:

- o arquivo participa de 01–12?
- é V1 ainda em produção ou V2 já homologada, porém ainda paralela?
- altera raw, stage, runtime, source_state, dist ou dist-v2?
- muda a relação entre conteúdo e `content_sha256`?
- perde `last_checked_at`, `last_successful_check_at`, `last_changed_at` ou competência?
- altera o manifesto ou permite JSON físico não declarado?
- enfraquece hash/tamanho/contrato?
- altera o conjunto exato das quatro fontes críticas?
- confunde publicação com validação?
- remove quarentena, lock, staging, swap atômico ou rollback?
- permitiria combinar artefatos de commits diferentes?
- reintroduz score, posição inventada, dupla contagem ou presença como qualidade?
- mudaria a semântica de `Segmentos_Consolidados` versus arquivos de grupos?
- quebra a capacidade de reproduzir a Auditoria 2?
- exige atualização deste memorial?

Se qualquer resposta for “sim”, a mudança é arquitetural e precisa de justificativa, teste e atualização do README.

## 16. Princípio final de segurança

Uma PR verde prova o código e suas fixtures. Uma release canônica no repositório prova o funil de geração. Uma release validada no HostGator prova a publicação operacional. **Nenhuma dessas etapas, isoladamente, prova que o frontend público já consome corretamente a V2.**

O aceite final deve vincular, sem saltos silenciosos:

**fonte → tentativa de coleta → estado durável → bytes consumidos → builder → manifesto → commit → release → validação HostGator → consumidor PHP/frontend.**

Essa cadeia é o contrato do projeto.
