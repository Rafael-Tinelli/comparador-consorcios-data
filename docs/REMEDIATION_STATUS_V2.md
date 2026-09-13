# Status de remediação — Auditoria 2 → Comparador V2

Referência: auditoria de 13/09/2026.

Este documento distingue quatro estados diferentes: correção implementada no repositório, validação em CI, homologação operacional no HostGator e ativação pública do consumidor/frontend.

## Situação resumida

- **Metodologia/read models V2:** implementados.
- **C09 real:** comprovado para as quatro fontes críticas.
- **Publicação HostGator paralela:** mecanismo homologado em 13/09/2026 sob o contrato histórico `comparador-v2-release.v1`.
- **Simplificação C15:** backend V2 alterado para release **data-only**, contrato `comparador-v2-release.v2` e pipeline `4.1.0`.
- **SEO V2:** deixa de ser responsabilidade do repositório de dados; pertence ao frontend/site.
- **Re-homologação do novo envelope data-only:** necessária antes do corte público.
- **Produção pública:** continua V1.
- **`deploy_enabled`:** permanece `false`.

## Mapa C01–C16

| ID | Status V2 | Tratamento / evidência | Pendência antes de produção pública |
|---|---|---|---|
| C01 | **Implementado; envelope v2 precisa re-homologação** | Manifesto continua sendo a lista auditável dos artefatos consumíveis. A release data-only contém sete JSONs globais não-meta + `meta.json`; SEO não faz mais parte do inventário. Pull/validator/rollback continuam usando o manifesto e o mesmo gate. | Rodar workflow 11 verde e re-homologar `comparador-v2-release.v2` no HostGator paralelo. |
| C02 | **Implementado; simplificado** | Builder data-only exige cadastro, filiais, mensal, ranking e metodologia. SEO deixou de ser insumo obrigatório. CI continua bloqueando núcleo ausente, hash/tamanho divergente, JSON extra, contrato incorreto e inventário físico divergente. | Confirmar novo envelope no workflow 11/HostGator. |
| C03 | **Implementado** | Estoques, fluxos e taxa vêm de `Segmentos_Consolidados`; grupos ficam restritos a enriquecimentos compatíveis/reconciliação. | Frontend deve consumir os novos campos. |
| C04 | **Implementado** | Zero e ausência são distintos; numerador incompleto não vira zero; não existe nota neutra 50 nem redistribuição de peso; segmento zerado não vira portfólio. | UI deve preservar os estados. |
| C05 | **Implementado** | `posicao_oficial` só existe se a fonte trouxer coluna explícita. Ordem de linha não vira ranking BC. | Remover rótulos/consumo legado no frontend. |
| C06 | **Backend preparado / consumidor pendente** | V2 não contém score geral e `deploy_v2` proíbe fallback metodológico silencioso. | PHP publicado precisa deixar de recorrer ao score V1. |
| C07 | **Dados implementados / UI pendente** | Campo `cotas_ativas_em_dia`; inadimplência V2 é participação `I/(N+I)` com semântica explícita. | Corrigir labels/formatter e casos-limite no frontend. |
| C08 | **Implementado no recorte de dados** | SGS 25497 corrigida para `% a.m.` e recorte específico. SGS permanece fonte auxiliar, não artefato da release V2 atual. | Se houver uso editorial, respeitar unidade/período e separar qualquer derivado. |
| C09 | **Implementado e comprovado em execução real / consumidor pendente** | Source-state persiste consulta, sucesso, mudança, hash, status e competência. Em 13/09/2026 as quatro fontes críticas rodaram com `bootstrap=false`; ConsorcioBD e ranking registraram `changed:false` sem avanço artificial de `last_changed_at`. | Frontend deve mostrar competência/atualidade a partir de `source_status`, não chamar `generated_at` de atualização da fonte. |
| C10 | **Resolvido por mudança de contrato** | V2 inicial não tem nota/ranking geral. `methodology_v2.json` é política executável e tem hash no meta. | Qualquer futura nota exige nova metodologia/versionamento/auditoria. |
| C11 | **Implementado** | Taxa por segmento usa consolidado; grupos enriquecem prazo/crédito; medianas derivadas não são chamadas de “mercado oficial”. | UI deve manter benchmark separado de oferta. |
| C12 | **Implementado no recorte atual** | Join de filiais por raiz; órfão `87945218` explícito; segmento 6 `Serviços turísticos`; catálogo atual separado de história operacional. | Lifecycle/sucessão somente com fonte temporal adequada. |
| C13 | **Mecanismo homologado sob v1; gate preservado em v2** | Lock comum, SHA imutável, staging, hash/tamanho, revalidação de `current-v2`, quarentena, swap atômico e rollback foram provados no host real. O contrato v2 mantém esses mecanismos e altera apenas o envelope de artefatos/release. | Repetir homologação estreita após publicar uma release data-only. |
| C14 | **Contrato de dados pronto / UI pendente** | Perfis carregam cobertura, sinais disponíveis, período e limites. | Renderizar explicação junto da comparação sem criar selo de qualidade. |
| C15 | **Fronteira backend implementada / consumidor público pendente** | Inventário real confirmou 13 URLs existentes e quatro destinos do antigo `seo_routes.json` ausentes. Decisão: não criar páginas por existência de JSON. V2 deixa de gerar `defaults/routes/site`; SEO editorial passa ao PHP/site (`head-global.php`, `config-site.php`, página dona do canonical/title/description). | Remover/deixar de carregar `_app/consorcio-seo.php` no comparador público, migrar SEO da raiz para o padrão nativo e confirmar URLs/canonicals/sitemap após o corte. |
| C16 | **Pendente de frontend** | Nenhum redesign foi embutido no backend. | Busca/URL, detalhes progressivos, 390 px, zoom 200%, teclado/foco, sem-JS e histórico precisam homologação. |

## Pipeline canônico V2 data-only

A cadeia nova é:

1. coletores gravam os insumos e persistem `data/source_state/*.json` nas quatro fontes críticas;
2. `transform/build_release_v2.py` constrói somente `data/dist-v2/global/*.json`;
3. o builder remove qualquer `data/dist-v2/seo/*.json` residual da geração V2 anterior;
4. `transform/finalize_v2_release.py` anexa proveniência e produz `backend_release.contract=comparador-v2-release.v2`;
5. `scripts/validate_v2_release.py` valida contratos, manifesto, hashes, semântica, source-state e `release_scope=data_only`;
6. o validador recusa JSON SEO físico ou declarado na release data-only;
7. PHP 8.2 aplica o mesmo manifesto/gate a pull, validate e rollback;
8. workflow 12 recusa stale base antes do commit da release;
9. HostGator V2 permanece isolado da V1 até o corte do consumidor.

### Artefatos consumíveis V2

Não-meta:

- `global/instituicoes.json`;
- `global/administradoras.json`;
- `global/produtos.json`;
- `global/rankings.json`;
- `global/segmentos.json`;
- `global/comparacoes.json`;
- `global/ofertas.json`.

Manifesto/estado da release:

- `global/meta.json`.

Não existem mais artefatos SEO consumíveis na V2. `meta.artifacts.seo=[]` é somente compatibilidade transitória com a biblioteca operacional que percorre as famílias `global` e `seo`; não representa contrato SEO nem arquivo físico.

## Decisão C15 — arquitetura do site

O snapshot read-only do HostGator confirmou que o comparador raiz é o consumidor específico dos helpers da ferramenta, enquanto as páginas filhas usam a arquitetura normal do site.

### 13 rotas reais confirmadas

- `/financas/consorcio/`;
- `/financas/consorcio/carro/`;
- `/financas/consorcio/imobiliario/`;
- `/financas/consorcio/moto/`;
- `/financas/consorcio/carta-contemplada/`;
- `/financas/consorcio/servicos/`;
- `/financas/consorcio/servicos/cirurgia-plastica/`;
- `/financas/consorcio/eletroeletronicos/`;
- `/financas/consorcio/moveis-planejados/`;
- `/financas/consorcio/moto/eletrica/`;
- `/financas/consorcio/moto/usada/`;
- `/financas/consorcio/moto/bradesco/`;
- `/financas/consorcio/moto/porto-seguro/`.

### Quatro destinos antigos confirmados como ausentes

- `/financas/consorcio/melhor-consorcio.php`;
- `/financas/consorcio/consorcio-ou-financiamento.php`;
- `/financas/consorcio/ranking-administradoras.php`;
- `/financas/consorcio/menor-taxa.php`.

A ausência desses arquivos não deve ser “corrigida” criando páginas automáticas. O defeito era a duplicação de autoridade de rota/configuração. O território “consórcio ou financiamento” já é editorial e não deve ser duplicado por fallback técnico.

### Autoridades depois da migração

- **Dados e proveniência:** `comparador-consorcios-data`;
- **SEO da página:** próprio PHP + `head-global.php`;
- **sitemap/navegação/arquitetura global:** `config-site.php`;
- **widget/comparação:** consumidor PHP/JS da release V2.

`_app/consorcio-seo.php` permanece somente na produção V1 até o frontend ser migrado; não deve ganhar uma nova função estrutural na V2.

## Compatibilidade V1 durante a transição

A simplificação V2 não altera o funil público atual:

- workflow 08 continua usando o builder V1;
- `config/seo_routes.json` continua existindo enquanto V1 precisar dele;
- `data/dist/seo/` continua sendo V1;
- `/home1/sanid210/consorcio-data/current` continua sendo a raiz pública de dados V1;
- nenhuma página pública foi alterada por esta remediação de repositório.

A remoção definitiva do legado só pode ocorrer depois do Marco C, com verificação de que nenhum consumidor restante o usa.

## Evidência operacional histórica — 13/09/2026

### GitHub Actions / C09

- workflow 12 real publicou uma release canônica válida;
- uma execução foi corretamente recusada por stale base depois de outro writer avançar `main`;
- execução posterior publicou sobre base estabilizada;
- repetição com inputs idênticos terminou sem commit artificial;
- cadastro e filiais tiveram sucesso com mudança;
- ConsorcioBD e reclamações tiveram sucesso `changed:false`, preservando `last_changed_at`.

Regra: após coletores manuais, aguardar writers derivados — especialmente workflow 08 — antes de iniciar workflow 12. Em stale-base, iniciar nova execução; não usar “Re-run failed jobs” para reaproveitar artefatos construídos sobre HEAD antigo.

### HostGator — contrato histórico v1

Instalação paralela:

`/home1/sanid210/comparador-consorcios-v2-homolog`

Baseline comprovado:

- PHP 8.2.33;
- origem `34b5f53e35499352086dc6dc94f9c8cc944507d3`;
- release `20260913T154448Z_34b5f53e_2009151a`;
- manifest `2009151aca2a6655027eadf6b38eb279d169b13ae6a20149a67a37e8c75eae78`;
- source fingerprint `89b47d7af9159b3aa7bebaa8ea7eb93ff7ad1e91ebc6831e9195dd777492a996`;
- release fingerprint `db7a2470c8edb517ceea14a9e2a343d71f5eae2e1e508401deaf6fd194ccfb68`;
- 10 artefatos não-meta naquela versão;
- `degraded_sources=[]`;
- dry-run, publicação, validação independente, `no_change/RC=10`, states e logs aprovados;
- nenhuma candidata foi para quarentena;
- segunda release local de bytes idênticos criada apenas para provar rollback/swap/gate;
- rollback e validação pós-rollback aprovados;
- V1 permaneceu em `/home1/sanid210/consorcio-data/releases/20260913T152026Z_d6066ac`.

Essa rodada é baseline histórico do mecanismo. A alteração para sete artefatos data-only exige nova prova operacional do **envelope**, não reabertura da metodologia.

## Gates atuais esperados

Na fixture auditada, a CI deve continuar provando:

- 130 administradoras;
- 368 operações observadas em 124 raízes;
- distribuição 72/56/108/67/28/37;
- imóveis: 2.818.836 cotas em dia, 13.771 contemplações, 311.836 inadimplentes, 2.779 grupos;
- 85 registros de reclamações;
- nenhuma posição oficial fabricada;
- órfão `87945218` explícito;
- `general_score=false` e nenhum score geral;
- sete artefatos de dados não-meta + meta;
- zero JSON SEO na release V2;
- inventário físico = manifesto;
- SHA-256 e tamanho recalculados;
- source-state das quatro fontes críticas;
- `release_scope=data_only`;
- `comparador-v2-release.v2`;
- validação PHP 8.2 e casos negativos de integridade/proveniência/state/lock/symlink.

## Critério de re-homologação do envelope data-only

Depois de workflow 11 verde e merge:

1. executar workflow 12 em `main` estabilizado;
2. confirmar que o commit canônico remove `data/dist-v2/seo/*.json` e publica o novo meta;
3. atualizar `hostgator/v2` na base paralela;
4. dry-run;
5. pull real da nova release;
6. confirmar `validated_artifacts=7`, `release_scope=data_only` e contrato v2;
7. confirmar ausência de `seo/*.json` na release;
8. validar `current-v2`;
9. conferir state/logs/quarentena;
10. confirmar `/home1/sanid210/consorcio-data/current` intacto.

## Critério de ativação pública

A V2 só vira produção após a migração do frontend, especialmente C06, C07, C14, C15 e C16. Até lá:

- `deploy_enabled=false`;
- V1 permanece pública;
- a instalação V2 continua paralela;
- nenhuma simplificação do backend autoriza troca automática do consumidor;
- SEO/editorial deve ser migrado para o padrão nativo do site antes da retirada de `consorcio-seo.php` da produção.
