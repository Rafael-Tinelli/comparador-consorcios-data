# Comparador de Consórcios Data

Backend canônico de dados do **Comparador de Consórcios Sanida**.

Este repositório produz e publica exclusivamente a release V2 data-only consumida pelo frontend em `https://sanida.com.br/financas/consorcio/`.

## 1. Estado canônico

A migração V1 → V2 foi encerrada. Não existe fallback metodológico ou operacional para V1.

Contratos vigentes:

- release: `comparador-v2-release.v2`;
- pipeline: `4.2.0`;
- metodologia: `2.1.0`;
- interpretação embutida: `interpretacao-relativa.v1`;
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

## 3. Fontes canônicas

A V2 usa somente quatro fontes operacionais:

| Fonte | Papel | Conteúdo consumido |
|---|---|---|
| `bc_cadastro_admins` | cadastro atual | `data/stage/cadastro/instituicoes_cadastro.json` |
| `bc_filiais` | presença cadastral informativa | `data/stage/filiais/filiais.json` |
| `bc_consorciobd` | operação mensal por segmento | `data/raw/bc/consorciobd/latest_source.bin` |
| `bc_ranking_reclamacoes` | reclamações BCB | `data/raw/bc/ranking_reclamacoes/latest_source.csv` |

`config/provenance_v2.json` define os quatro estados duráveis C09.

SGS, ConsorcioBD trimestral e ABAC não integram a release V2 final e foram removidos do pipeline canônico.

## 4. Camadas de dados

- `data/raw/` — bytes e snapshots aprovados das fontes;
- `data/stage/` — normalizações intermediárias;
- `data/runtime/` — estado efêmero de execução, não canônico;
- `data/source_state/` — memória durável de consulta/sucesso/mudança/competência/hash;
- `data/dist-v2/global/` — única superfície publicável.

Não existe `data/dist/` V1 nem `data/dist-v2/seo/` publicável.

## 5. Read models publicados

A release contém sete JSONs globais mais o manifesto:

- `instituicoes.json` — `instituicoes.v2`;
- `administradoras.json` — `administradoras.v2`;
- `produtos.json` — `produtos.v2`;
- `rankings.json` — `rankings.v2`;
- `segmentos.json` — `segmentos.v2`;
- `comparacoes.json` — `comparacoes.v2`;
- `ofertas.json` — `ofertas.v2`;
- `meta.json` — manifesto, metodologia, proveniência e release.

`interpretacao-relativa.v1` é embutida em `administradoras`, `segmentos` e `comparacoes`; não cria um oitavo read model.

## 6. Transformação canônica

Executável:

```bash
python transform/build_release_v2.py \
  --config config/sources.json \
  --methodology config/methodology_v2.json
```

Núcleo puro:

`transform/read_models_v2_core.py`

O núcleo não possui CLI, geração SEO ou compatibilidade V1. O orquestrador aplica `interpretation_v2.py` somente depois da validação dos modelos-base.

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

## 7. Workflows

A superfície operacional final é:

1. `01-coleta-bc-cadastro.yml` — cadastro;
2. `02-coleta-bc-filiais.yml` — filiais;
3. `03-coleta-consorciobd.yml` — ConsorcioBD mensal;
4. `04-coleta-ranking-reclamacoes.yml` — reclamações BCB;
5. `05-validate-v2.yml` — contratos, testes e gates;
6. `06-build-publish-v2.yml` — geração e commit da release canônica.

Não existem workflows de build/deploy V1.

## 8. Publicação HostGator

A biblioteca canônica permanece em `hostgator/v2/` e opera sobre:

- `releases-v2/`;
- `current-v2`;
- `_tmp-v2/`;
- `state-v2/`;
- `logs-v2/`;
- `locks-v2/`.

O sufixo `-v2` identifica o contrato da release; não representa coexistência com V1.

`current-v2` deve ser symlink para uma release validada. O frontend público lê somente `current-v2` e nunca tenta `current`/V1 como fallback.

## 9. Invariantes metodológicos

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
- `generated_at` não substitui competência nem freshness da fonte.

## 10. Gates de publicação

Uma release só pode avançar quando:

- os sete contratos globais estão presentes;
- manifesto cobre todos os artefatos não-meta;
- SHA-256 e tamanho conferem;
- inventário físico coincide com manifesto;
- `general_score=false` e `general_ranking=false`;
- nenhuma superfície SEO está publicada pelo backend;
- os quatro `source_state` obrigatórios existem e correspondem aos bytes consumidos;
- competência está resolvida conforme a fonte;
- `backend_release.contract=comparador-v2-release.v2`;
- `publication_eligible=true`;
- testes Python e PHP passam.

## 11. Política de mudanças

Alterações em metodologia, contratos, fontes, proveniência, publicação, rollback ou fronteiras backend/frontend devem atualizar este README na mesma PR.

Não reintroduzir:

- `data/dist` V1;
- `deploy.json` V1;
- `scoring_rules.json`;
- `seo_routes.json` no backend;
- builder V1;
- fallback V1 no frontend;
- score/ranking geral sem nova metodologia versionada e auditoria.
