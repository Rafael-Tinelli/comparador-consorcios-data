# Status de remediação — Auditoria 2 → Comparador V2

Referência de trabalho: auditoria de 13/09/2026. Este documento distingue correção implementada no repositório de homologação de correção já instalada no HostGator/frontend.

| ID | Status V2 | Tratamento nesta PR | Pendência antes de produção |
|---|---|---|---|
| C01 | **Implementado na V2 / não instalado** | Manifesto passa a declarar todos os JSONs não-meta. Pull, validator e rollback V2 usam o mesmo inventário derivado do manifesto. Scripts HostGator V2 agora são versionados. | Instalar `hostgator/v2` e migrar `current-v2` para o consumidor somente após homologação. |
| C02 | **Implementado na V2** | Builder exige cadastro, filiais, mensal, ranking, metodologia e rotas. CI bloqueia núcleo ausente, hash/tamanho divergente, JSON extra, contrato incorreto e inventário físico divergente. | Repetir os testes no host real antes de ativar. |
| C03 | **Implementado** | Estoques, fluxos e taxa vêm exclusivamente de `Segmentos_Consolidados`. Grupos ficam restritos a prazo/crédito/reconciliação. | Nenhuma para o backend V2; frontend deve usar os novos campos. |
| C04 | **Implementado** | Zero e ausência são estados diferentes; numerador incompleto não vira zero; não existe nota 50 nem redistribuição de peso. Combinações raiz×segmento zeradas não viram portfólio. | UI deve preservar esses estados sem inventar qualidade. |
| C05 | **Implementado** | `posicao_oficial` só existe se a fonte trouxer coluna explícita. Ordem de linha nunca vira posição BC. | Remover rótulo/consumo legado no frontend. |
| C06 | **Backend preparado / consumidor pendente** | Contrato V2 não contém score legado; `deploy_v2` proíbe fallback silencioso. | PHP publicado deve deixar de substituir V2 por `instituicoes.json`/score antigo. |
| C07 | **Dados implementados / UI pendente** | Campo é `cotas_ativas_em_dia`; inadimplência V2 é participação `I/(N+I)` com unidade explícita no contrato. | Corrigir labels/formatter PHP e testar casos >100% do ratio legado sem reaproveitar heurística. |
| C08 | **Implementado** | SGS 25497 corrigida para `% a.m.` e recorte específico de financiamento imobiliário PF com recursos direcionados/taxas de mercado. | Se anualizar, publicar derivado separado. |
| C09 | **Parcial** | `generated_at` e `source_periods.consorciobd_mensal` são campos distintos; a V2 não chama geração de atualização da fonte. | Persistir `last_checked_at` por fonte mesmo quando `changed:false`; transportar coleta/mudança/competência para o consumidor. |
| C10 | **Resolvido por mudança de contrato** | V2 inicial não possui nota/ranking geral. `methodology_v2.json` é fonte executável e tem hash no `meta`. | Qualquer futura nota composta exige nova versão metodológica, elegibilidade e validação próprias. |
| C11 | **Implementado** | Taxa por segmento reproduz o consolidado; grupos enriquecem somente prazo/crédito; medianas derivadas são nomeadas como medianas das administradoras observadas, não “mercado oficial”. | UI deve manter benchmark separado de oferta comercial. |
| C12 | **Implementado no recorte atual** | Join de filiais por raiz; órfão `87945218` registrado explicitamente; código 6 rotulado `Serviços turísticos`; catálogo atual separado da história operacional. | Investigar lifecycle/sucessão somente quando houver fonte temporal apropriada; não inferir. |
| C13 | **Implementado na camada V2 / não instalado** | Mesmo lock para pull/validate/rollback; commit remoto resolvido antes de baixar; current revalidado mesmo sem mudança; rejeitadas entram em quarentena; symlink swap atômico. | Ensaiar interrupção real no HostGator e ativar apenas após validação. |
| C14 | **Contrato pronto / UI pendente** | Perfis informam cobertura, sinais disponíveis, limites da inferência, período e dimensões comparáveis. | Renderizar explicação próxima da comparação sem transformar triagem em selo. |
| C15 | **Parcial** | README e contratos V2 foram alinhados; SEO V2 limita-se a `defaults/routes/site` declarados. | Revisar consumidores/rotas SEO reais durante migração do frontend e eliminar destinos sem contrato confirmado. |
| C16 | **Pendente de frontend** | Sem alteração estética nesta PR. | Busca/URL, detalhes progressivos, 390 px, zoom 200%, teclado/foco e sem-JS devem ser homologados no frontend publicado. |

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
- validação PHP 8.2 da release e testes negativos de integridade/estado/lock/symlink.

## Critério de ativação

A PR pode ser revisada/mesclada como **fundação V2 sem ativação de produção**. A troca do comparador publicado só deve ocorrer quando o frontend PHP consumir os contratos V2, os scripts `hostgator/v2` estiverem instalados em paralelo e uma release concreta passar pelos gates no PHP 8.2 do HostGator.
