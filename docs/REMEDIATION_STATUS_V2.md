# Status de remediação — Auditoria 2 → Comparador V2

Referência de trabalho: auditoria de 13/09/2026. Este documento distingue correção implementada no repositório, homologação operacional no HostGator e pendência de migração do consumidor/frontend.

## Situação resumida

- **Backend V2 em código:** concluído.
- **Geração canônica V2 no GitHub:** concluída e comprovada em workflow real.
- **C09 em execução real:** concluído para as quatro fontes críticas.
- **HostGator V2 paralelo:** instalado e homologado operacionalmente em PHP 8.2 real.
- **Produção pública V2:** ainda não ativada.
- **V1 pública:** permanece intacta até a migração e homologação do consumidor/frontend.

| ID | Status V2 | Tratamento / evidência atual | Pendência antes de produção pública |
|---|---|---|---|
| C01 | **Implementado e homologado no HostGator** | Manifesto declara todos os JSONs não-meta. Pull, validator e rollback V2 usam o mesmo inventário derivado do manifesto. Scripts `hostgator/v2` estão versionados, foram instalados no HostGator real e uma release de 10 artefatos foi publicada e validada. | Migrar o consumidor para `current-v2` somente depois da homologação do frontend. |
| C02 | **Implementado e repetido no host real** | Builder exige cadastro, filiais, mensal, ranking, metodologia e rotas. CI bloqueia núcleo ausente, hash/tamanho divergente, JSON extra, contrato incorreto e inventário físico divergente. A release concreta também passou pelo gate PHP 8.2 no HostGator. | Nenhuma pendência de backend; preservar os mesmos gates durante a migração do consumidor. |
| C03 | **Implementado** | Estoques, fluxos e taxa vêm exclusivamente de `Segmentos_Consolidados`. Grupos ficam restritos a prazo/crédito/reconciliação. | Nenhuma para o backend V2; frontend deve usar os novos campos. |
| C04 | **Implementado** | Zero e ausência são estados diferentes; numerador incompleto não vira zero; não existe nota 50 nem redistribuição de peso. Combinações raiz×segmento zeradas não viram portfólio. | UI deve preservar esses estados sem inventar qualidade. |
| C05 | **Implementado** | `posicao_oficial` só existe se a fonte trouxer coluna explícita. Ordem de linha nunca vira posição BC. | Remover rótulo/consumo legado no frontend. |
| C06 | **Backend preparado / consumidor pendente** | Contrato V2 não contém score legado; `deploy_v2` proíbe fallback silencioso. | PHP publicado deve deixar de substituir V2 por `instituicoes.json`/score antigo. |
| C07 | **Dados implementados / UI pendente** | Campo é `cotas_ativas_em_dia`; inadimplência V2 é participação `I/(N+I)` com unidade explícita no contrato. | Corrigir labels/formatter PHP e testar casos >100% do ratio legado sem reaproveitar heurística. |
| C08 | **Implementado** | SGS 25497 corrigida para `% a.m.` e recorte específico de financiamento imobiliário PF com recursos direcionados/taxas de mercado. | Se anualizar, publicar derivado separado. |
| C09 | **Implementado no backend e comprovado em execução real / consumidor pendente** | Cada fonte crítica possui estado persistente separado dos dados (`last_checked_at`, `last_successful_check_at`, `last_changed_at`, hash do conteúdo, status da última consulta e competência). Em 13/09/2026 as quatro fontes críticas foram executadas de verdade com `bootstrap=false`; ConsorcioBD e ranking registraram `changed:false` sem avanço artificial de `last_changed_at`. O workflow 12 publicou release canônica reconciliando esses estados com os bytes consumidos. | Frontend/PHP deve apresentar competência/atualidade usando `source_status`, sem converter `generated_at` em “dados atualizados em”. |
| C10 | **Resolvido por mudança de contrato** | V2 inicial não possui nota/ranking geral. `methodology_v2.json` é fonte executável e tem hash no `meta`. | Qualquer futura nota composta exige nova versão metodológica, elegibilidade e validação próprias. |
| C11 | **Implementado** | Taxa por segmento reproduz o consolidado; grupos enriquecem somente prazo/crédito; medianas derivadas são nomeadas como medianas das administradoras observadas, não “mercado oficial”. | UI deve manter benchmark separado de oferta comercial. |
| C12 | **Implementado no recorte atual** | Join de filiais por raiz; órfão `87945218` registrado explicitamente; código 6 rotulado `Serviços turísticos`; catálogo atual separado da história operacional. | Investigar lifecycle/sucessão somente quando houver fonte temporal apropriada; não inferir. |
| C13 | **Implementado e homologado operacionalmente no HostGator** | Mesmo lock para pull/validate/rollback; commit remoto resolvido antes de baixar; `current-v2` revalidado mesmo sem mudança; rejeitadas entram em quarentena; symlink swap atômico. No HostGator real foram comprovados dry-run, publicação, validação independente, `no_change` idempotente, segunda promoção local, rollback manual e revalidação pós-rollback. Tentativa de publicação permanece separada da validação da release ativa. | Não houve fault-injection por interrupção abrupta do processo no host; esse hardening permanece coberto por testes negativos em CI e pode ser repetido futuramente se necessário. Não bloquear ativação do frontend sem nova evidência objetiva de risco. |
| C14 | **Contrato pronto / UI pendente** | Perfis informam cobertura, sinais disponíveis, limites da inferência, período e dimensões comparáveis. | Renderizar explicação próxima da comparação sem transformar triagem em selo. |
| C15 | **Parcial** | README e contratos V2 estão alinhados; SEO V2 limita-se a `defaults/routes/site` declarados. Backend/HostGator já foram homologados sem ampliar o inventário silenciosamente. | Revisar consumidores PHP e rotas SEO efetivamente publicadas durante a migração do frontend e eliminar destinos sem contrato confirmado. |
| C16 | **Pendente de frontend** | Sem alteração estética nesta fase. | Busca/URL, detalhes progressivos, 390 px, zoom 200%, teclado/foco e sem-JS devem ser homologados no frontend publicado. |

## Pipeline canônico do backend V2

O backend V2 deixa de compartilhar `data/dist` com a geração legada. A cadeia canônica é:

1. coletores gravam conteúdo somente quando o conteúdo muda e gravam `data/source_state/*.json` em toda tentativa;
2. `12 · Build & Publish Comparador V2` constrói em `data/dist-v2`;
3. `transform/finalize_v2_release.py` anexa a proveniência persistente ao `global/meta.json`;
4. `scripts/validate_v2_release.py` valida contratos, inventário, hashes, semântica e estados de fonte;
5. PHP 8.2 executa o mesmo contrato de release/proveniência usado por pull, validate e rollback;
6. o workflow recusa publicar se a branch avançar durante o build, preservando a relação entre commit de entrada e release gerada;
7. HostGator V2 aponta exclusivamente para `data/dist-v2` e permanece separado da produção pública V1 até a migração do consumidor.

## Evidência operacional real — 13/09/2026

### GitHub Actions

- workflow `12 · Build & Publish Comparador V2` executado em produção real do repositório;
- uma execução válida foi recusada no gate de stale base porque outro writer avançou `main` durante o build, comprovando que uma candidata construída sobre base obsoleta não é publicada;
- a execução seguinte, com a branch estabilizada, publicou a release canônica;
- uma execução posterior sobre a mesma base concluiu verde e registrou `V2 já está canônica; nenhum commit necessário.`, comprovando determinismo e ausência de commit artificial;
- regra operacional: depois de disparos manuais de coletores, aguardar writers derivados — especialmente workflow 08 — terminarem antes de iniciar manualmente o 12; em falha por stale base, iniciar um novo workflow 12 a partir do `main` atual em vez de usar “Re-run failed jobs”.

### Estados de fonte C09

As quatro fontes críticas foram executadas com estado real, sem bootstrap:

- `bc_cadastro_admins` — sucesso e mudança registrada;
- `bc_filiais` — sucesso e mudança registrada;
- `bc_consorciobd` — sucesso com `changed:false`, preservando `last_changed_at`;
- `bc_ranking_reclamacoes` — sucesso com `changed:false`, preservando `last_changed_at`.

Essa rodada foi consumida pelo workflow 12 e reconciliada com os bytes efetivamente usados no builder.

### HostGator real

Instalação paralela:

`/home1/sanid210/comparador-consorcios-v2-homolog`

Ambiente e release homologada:

- PHP CLI: `8.2.33`;
- commit canônico de origem: `34b5f53e35499352086dc6dc94f9c8cc944507d3`;
- release: `20260913T154448Z_34b5f53e_2009151a`;
- `manifest_sha256`: `2009151aca2a6655027eadf6b38eb279d169b13ae6a20149a67a37e8c75eae78`;
- `source_fingerprint`: `89b47d7af9159b3aa7bebaa8ea7eb93ff7ad1e91ebc6831e9195dd777492a996`;
- `release_fingerprint`: `db7a2470c8edb517ceea14a9e2a343d71f5eae2e1e508401deaf6fd194ccfb68`;
- `validated_artifacts=10`;
- `degraded_sources=[]`.

Sequência aprovada:

1. instalação de `hostgator/v2` em `bin-v2/`;
2. `php -l` de todos os scripts;
3. dry-run, sem criação de `current-v2`;
4. publicação real em `releases-v2`;
5. criação atômica de `current-v2`;
6. validação independente do current pelo mesmo gate;
7. segundo pull sem mudança com `RC=10`, `result=no_change` e `reason=remote_manifest_unchanged_current_healthy`;
8. inspeção de `current_release.json`, `last_publication_*`, `last_validation_*` e logs;
9. confirmação de ausência de `rejected_releases.json`, coerente com nenhuma candidata rejeitada;
10. criação controlada, via `--force`, de uma segunda release local com bytes/manifesto/fingerprint idênticos exclusivamente para ensaiar o mecanismo de rollback;
11. rollback explícito para a primeira release;
12. revalidação pós-rollback com sucesso;
13. confirmação de que a V1 permaneceu intacta.

A V1 permaneceu em:

`/home1/sanid210/consorcio-data/releases/20260913T152026Z_d6066ac`

O ensaio de rollback comprova staging, promoção, symlink swap, gate, rollback e revalidação. Como as duas releases locais do ensaio tinham bytes idênticos, ele **não** deve ser descrito como prova de reversão entre dois conteúdos de dados diferentes.

## Gates já automatizados

A CI V2 exige, na base auditada:

- 130 administradoras no catálogo atual;
- 368 combinações raiz×segmento com operação observada em 124 raízes, e não as 762 combinações preenchidas do consolidado;
- distribuição de segmentos 72/56/108/67/28/37;
- imóveis reconciliados em 2.818.836 cotas em dia, 13.771 contemplações, 311.836 inadimplentes e 2.779 grupos;
- 85 registros de reclamações e nenhuma posição oficial inventada;
- órfão `87945218` explicitamente registrado;
- `general_score=false` e nenhum campo de score nos perfis;
- inventário físico = manifesto, com SHA-256 recalculado;
- estado persistente para cadastro, filiais, ConsorcioBD e reclamações, com consulta/mudança/competência separadas;
- contrato `comparador-v2-release.v1` e release fingerprint;
- validação PHP 8.2 da release e testes negativos de integridade, proveniência, estado, lock e symlink;
- tentativa de publicação separada da validação da release ativa.

## Critério de ativação

O **backend V2 está fechado em código e homologado operacionalmente no HostGator** no escopo definido pelo README. Isso ainda **não significa produção pública ativada**.

A troca do comparador publicado permanece condicionada à migração e homologação do consumidor PHP/frontend para os contratos V2, especialmente:

- C06 — remover fallback/score legado;
- C07 — corrigir labels, unidades, datas e formatters;
- C14 — renderizar cobertura, período, motivos e limites da comparação;
- C15 — reconciliar consumidores e rotas SEO reais;
- C16 — homologar acessibilidade, responsividade, histórico/estado e comportamento sem JS.

Até esse fechamento:

- `config/deploy_v2.json` deve permanecer com `deploy_enabled=false`;
- V1 continua sendo a produção pública;
- a instalação V2 homologada deve permanecer paralela;
- a pasta de homologação e as duas releases V2 locais devem ser preservadas enquanto forem úteis como baseline operacional da migração do consumidor.
