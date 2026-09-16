# Comparador de Consórcios Data

Backend canônico de dados do **Comparador de Consórcios Sanida**.

Este repositório produz e publica exclusivamente a release V2 data-only consumida pelo frontend em `https://sanida.com.br/financas/consorcio/`.

## 1. Estado canônico

A migração V1 → V2 foi encerrada. Não existe fallback metodológico ou operacional para V1.

Contratos vigentes:

- release: `comparador-v2-release.v2`;
- pipeline: `4.3.0`;
- metodologia: `2.1.0`;
- interpretação embutida: `interpretacao-relativa.v1`;
- taxonomia de segmentos: `segment-taxonomy.v1`;
- `general_score=false`;
- `general_ranking=false`;
- `release_scope.kind=data_only`;
- `seo_artifacts=false`;
- `seo_owner=frontend_site`.

Rollback significa voltar para uma **release V2 previamente aprovada**. V1 não é alvo de rollback.

## 2. Fronteira de responsabilidade

### Backend deste repositório

Responsável por:

- coleta das fontes necessárias;
- preservação dos insumos;
- normalização e read models;
- taxonomia oficial aditiva de segmentos;
- contratos de dados;
- interpretação relativa auditável;
- proveniência e freshness;
- manifesto, hashes e release fingerprint;
- validação, publicação atômica e rollback entre releases V2.

Não publica:

- title/meta description;
- canonical;
- breadcrumbs;
- FAQ editorial;
- sitemap;
- páginas SEO;
- score ou ranking geral.

### Frontend/site Sanida

Responsável pela página pública, arquitetura editorial, includes globais, SEO e experiência do usuário.

O frontend pode ordenar somente por posições/ordens já publicadas pelo backend. Não deve recalcular mediana, quartis, missingness, ranking ou metodologia.

Uma categoria nova na fonte oficial pode aparecer automaticamente na interface genérica. A criação de uma URL editorial dedicada continua sendo decisão humana, condicionada a demanda, tese própria e utilidade suficiente.

## 3. Fontes canônicas

A V2 usa quatro fontes operacionais:

| Fonte | Papel | Conteúdo consumido |
|---|---|---|
| `bc_cadastro_admins` | cadastro atual | `data/stage/cadastro/instituicoes_cadastro.json` |
| `bc_filiais` | presença cadastral informativa | `data/stage/filiais/filiais.json` |
| `bc_consorciobd` | operação mensal por segmento | `data/raw/bc/consorciobd/latest_source.bin` |
| `bc_ranking_reclamacoes` | reclamações BCB | `data/raw/bc/ranking_reclamacoes/latest_source.csv` |

A própria página oficial do ConsorcioBD fornece a legenda código → segmento. O workflow `03-coleta-consorciobd.yml` coleta essa taxonomia e mantém `data/stage/consorciobd/segment_taxonomy.json`.

`config/provenance_v2.json` define os quatro estados duráveis C09.

SGS, ConsorcioBD trimestral e ABAC não integram a release V2 final e foram removidos do pipeline canônico.

## 4. Taxonomia oficial evergreen

A aplicação não trata mais `{1,2,3,4,5,6}` como uma lista imutável escrita no validador.

Política:

- **adição oficial de novo código:** automática;
- **nova administradora em segmento conhecido:** automática;
- **administradora passa a operar em novo segmento:** automática;
- **mudanças normais de competência e métricas:** automáticas;
- **desaparecimento de código existente:** não remove silenciosamente a ontologia;
- **mudança material do significado de um código existente:** não sobrescreve silenciosamente;
- **fusão/substituição/reclassificação de códigos:** exige revisão de migração.

Keys técnicas já publicadas permanecem estáveis. O payload de stage preserva separadamente `label_oficial` e `label_exibicao`.

Novos códigos oficiais propagam automaticamente para:

- `produtos.json`;
- portfólio observado das administradoras;
- `segmentos.json`;
- `comparacoes.json`;
- filtros e módulos genéricos que consomem esses read models.

A taxonomia **não cria páginas SEO automaticamente**.

## 5. Camadas de dados

- `data/raw/` — bytes e snapshots aprovados das fontes;
- `data/stage/` — normalizações intermediárias e taxonomia oficial persistida;
- `data/runtime/` — estado efêmero de execução, não canônico;
- `data/source_state/` — memória durável de consulta/sucesso/mudança/competência/hash;
- `data/dist-v2/global/` — única superfície publicável.

Não existe `data/dist/` V1 nem `data/dist-v2/seo/` publicável.

## 6. Read models publicados

A release contém sete JSONs globais mais o manifesto:

- `instituicoes.json` — `instituicoes.v2`;
- `administradoras.json` — `administradoras.v2`;
- `produtos.json` — `produtos.v2`;
- `rankings.json` — `rankings.v2`;
- `segmentos.json` — `segmentos.v2`;
- `comparacoes.json` — `comparacoes.v2`;
- `ofertas.json` — `ofertas.v2`;
- `meta.json` — manifesto, metodologia, proveniência, taxonomia e release.

`interpretacao-relativa.v1` é embutida em `administradoras`, `segmentos` e `comparacoes`; não cria um oitavo read model.

## 7. Transformação canônica

Executável:

```bash
python transform/build_release_v2.py \
  --config config/sources.json \
  --methodology config/methodology_v2.json
```

Núcleo puro:

`transform/read_models_v2_core.py`

Contrato de taxonomia:

`transform/segment_taxonomy_v2.py`

Coletor da legenda oficial:

`collectors/bc_segment_taxonomy.py`

O núcleo não possui CLI, geração SEO ou compatibilidade V1. O orquestrador injeta a taxonomia oficial antes de ler o ConsorcioBD e aplica `interpretation_v2.py` somente depois da validação dos modelos-base.

Depois do build:

```bash
python transform/finalize_v2_release.py \
  --dist-base data/dist-v2 \
  --provenance-config config/provenance_v2.json

python scripts/validate_v2_release.py \
  --dist-base data/dist-v2 \
  --provenance-config config/provenance_v2.json \
  --deploy-config config/deploy_v2.json
```

## 8. Workflows

A superfície operacional final é:

1. `01-coleta-bc-cadastro.yml` — cadastro;
2. `02-coleta-bc-filiais.yml` — filiais;
3. `03-coleta-consorciobd.yml` — ConsorcioBD mensal + taxonomia oficial;
4. `04-coleta-ranking-reclamacoes.yml` — reclamações BCB;
5. `05-validate-v2.yml` — contratos, testes e gates;
6. `06-build-publish-v2.yml` — geração e commit da release canônica.

Não existem workflows de build/deploy V1.

## 9. Publicação HostGator

A biblioteca-fonte canônica permanece em `hostgator/v2/`. Em produção, os scripts correspondentes ficam instalados sob `/home1/sanid210/consorcio-data/bin-v2/` e operam sobre:

- `releases-v2/`;
- `current-v2`;
- `_tmp-v2/`;
- `state-v2/`;
- `logs-v2/`;
- `locks-v2/`.

`/home1/sanid210/consorcio-data/current-v2` é o único ponteiro ativo consumido pelo frontend público. Não existe fallback para `current`/V1.

Rollback é exclusivamente V2 → V2 previamente aprovada.

## 10. Invariantes metodológicos

- zero é diferente de ausência;
- ausência não recebe zero, nota neutra nem redistribuição de peso;
- índice BCB não divulgado não equivale a zero reclamações;
- reclamações BCB são institucionais, não segmentadas;
- taxa observada no ConsorcioBD não é oferta comercial atual;
- contemplações mensais são volume absoluto e não permitem inferir chance, prazo ou velocidade de contemplação;
- porte/presença não é qualidade;
- `ranking_geral_publicavel=false`;
- ordem por critério é derivada e `oficial=false`;
- comparação operacional exige mesmo segmento e mesma competência;
- `generated_at` não substitui competência nem freshness da fonte;
- classificação comercial de administradoras não substitui segmento oficial do BCB.

## 11. Gates de publicação

Uma release só pode avançar quando:

- os sete contratos globais estão presentes;
- manifesto cobre todos os artefatos não-meta;
- SHA-256 e tamanho conferem;
- inventário físico coincide com manifesto;
- `general_score=false` e `general_ranking=false`;
- nenhuma superfície SEO está publicada pelo backend;
- os quatro `source_state` obrigatórios existem e correspondem aos bytes consumidos;
- competência está resolvida conforme a fonte;
- todos os códigos observados no ConsorcioBD pertencem à taxonomia oficial coletada;
- a taxonomia está em estado `ok` ou `ok_with_additions`;
- adições oficiais podem avançar sem intervenção manual;
- remoções/renomeações estruturais não são publicadas silenciosamente;
- `backend_release.contract=comparador-v2-release.v2`;
- `publication_eligible=true`;
- testes Python e PHP passam.

## 12. Política de mudanças

Alterações em metodologia, contratos, fontes, taxonomia, proveniência, publicação, rollback ou fronteiras backend/frontend devem atualizar este README na mesma PR.

Não reintroduzir:

- `data/dist` V1;
- `deploy.json` V1;
- `scoring_rules.json`;
- `seo_routes.json` no backend;
- builder V1;
- fallback V1 no frontend;
- score/ranking geral sem nova metodologia versionada e auditoria;
- categorias comerciais apresentadas como se fossem segmentos oficiais do BCB.
