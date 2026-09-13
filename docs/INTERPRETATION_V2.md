# Interpretação relativa V2 — contrato para o frontend

## Objetivo

A camada `interpretacao-relativa.v1` transforma métricas públicas já validadas em contexto comparável para o frontend sem criar score geral, recomendação universal ou inferência comercial.

Ela existe para responder melhor a perguntas reais do usuário — por exemplo, se uma administradora possui evidência pública suficiente para triagem, se uma taxa observada está acima ou abaixo da referência do segmento, qual é a escala relativa da operação e o que os dados de contemplação não permitem concluir.

A camada é **dados/metodologia**, não SEO editorial. O frontend continua dono de title, description, H1, canonical, URLs, schema editorial, composição visual e copy de página.

## Contrato

- contrato embutido: `interpretacao-relativa.v1`;
- metodologia: `2.1.0`;
- pipeline: `4.2.0`;
- não cria novo JSON global: continua havendo sete artefatos não-meta;
- aparece de forma aditiva em `administradoras.v2`, `segmentos.v2` e `comparacoes.v2`;
- `meta.embedded_contracts.interpretacao_relativa` identifica o subcontrato.

## Invariantes

1. Não existe score geral.
2. Não existe ranking geral de qualidade.
3. Ordem derivada é sempre por **um critério explicitamente identificado**.
4. Ordem derivada nunca é marcada como posição oficial do Banco Central.
5. Missing não recebe zero, nota neutra, posição ou sinal favorável.
6. Porte/escala não é qualidade.
7. Contemplações mensais são volume absoluto e não estimam chance individual nem tempo de contemplação.
8. Taxa observada no ConsorcioBD não é oferta comercial atual.
9. Índice de reclamações BCB é institucional; não é específico de um segmento.
10. Comparações operacionais usam o mesmo segmento e a mesma competência.

## Referências estatísticas

Para cada segmento e métrica operacional são calculados:

- `n`;
- `min`;
- `q1`;
- `mediana`;
- `q3`;
- `max`.

Os quartis usam interpolação linear na posição `p * (n - 1)`. O objetivo é oferecer referência relativa estável e auditável, não normalizar tudo em uma nota.

A faixa de distribuição pode ser:

- `quartil_inferior`;
- `entre_q1_e_mediana`;
- `mediana`;
- `entre_mediana_e_q3`;
- `quartil_superior`;
- `amostra_insuficiente_para_quartis`.

O frontend pode usar essas faixas como pistas visuais, mas não deve converter automaticamente `quartil_inferior` ou `quartil_superior` em verde/vermelho. A semântica depende da métrica.

## Métricas por segmento

### Taxa de administração observada

Campo: `taxa_administracao_pct`.

- ordem derivada permitida: menor primeiro;
- papel: custo observado;
- comparação: somente entre administradoras com valor disponível no mesmo segmento/competência;
- limite: não é proposta comercial atual.

### Cotas ativas em dia

Campo: `cotas_ativas_em_dia`.

- ordem derivada permitida: maior primeiro;
- papel: escala operacional;
- limite: porte não é qualidade, confiabilidade nem disponibilidade comercial.

### Contemplações no mês

Campo: `contemplacoes_mes`.

- ordem derivada permitida: maior primeiro;
- papel: volume absoluto observado;
- limite obrigatório: não permite afirmar quem contempla mais rápido nem estimar probabilidade individual.

### Participação calculada de inadimplência

Campo: `inadimplencia_participacao`.

- ordem derivada permitida: menor primeiro;
- papel: indicador derivado dos estoques publicados;
- fórmula continua sendo `inadimplentes/(cotas_ativas_em_dia+inadimplentes)`;
- missing não pode virar zero.

## Reclamações BCB

O contexto do índice de reclamações é calculado entre administradoras do cadastro atual que possuem índice divulgado.

- ordem derivada: menor índice primeiro;
- `oficial=false` na ordem derivada;
- índice não divulgado permanece indisponível com motivo;
- ausência do índice não significa zero nem ausência de reclamações;
- a referência é institucional e não segmentada.

A distribuição global usada como benchmark aparece em `administradoras.metadata.reclamacoes_bcb_referencia_global`.

## Leitura de confiabilidade

`administradoras[].leitura_confiabilidade` passa a incluir `contract`, `resposta_chave`, `texto`, `sinais` e `limites`.

As respostas possíveis são leituras de **suficiência de evidência**, não certificados de confiabilidade:

- `evidencia_ampla_para_triagem`;
- `evidencia_ampla_com_indice_nao_divulgado`;
- `evidencia_operacional_sem_registro_reclamacoes_vinculado`;
- `evidencia_reclamacoes_sem_operacao_mensal_observada`;
- `apenas_cadastro_atual`.

O frontend pode formular uma pergunta editorial como “A administradora X é confiável?”, mas a resposta deve preservar a conclusão do contrato: evidência para triagem, sinais disponíveis e limites. Não transformar `evidencia_ampla_para_triagem` em “sim, é confiável”.

## Estrutura em `comparacoes.v2`

Cada segmento contém:

```text
interpretacao
  contract
  general_ranking=false
  criterios
    taxa_administracao_pct
    cotas_ativas_em_dia
    contemplacoes_mes
    inadimplencia_participacao
  contemplacao
```

Cada linha de administradora contém:

```text
interpretacao_relativa
  contract
  metricas_segmento
    <metrica>
      availability
      reason
      mediana_referencia
      comparacao_mediana
      diferenca_mediana
      diferenca_mediana_relativa
      faixa_distribuicao
      ordem_derivada
      destaque
      papel
      escopo
  reclamacoes_bcb
```

`ordem_derivada` tem `posicao`, `universo`, `sentido`, `oficial=false` e nota explicativa.

## Estrutura em `segmentos.v2`

Cada segmento contém `interpretacao.referencias`, com a distribuição completa de cada métrica e a política explícita sobre contemplação.

Isso permite construir páginas/visões de segmento sem recalcular estatística no PHP ou JavaScript.

## Estrutura em `administradoras.v2`

Além da leitura de confiabilidade, cada item de `portfolio_observado.segmentos[]` recebe `interpretacao_relativa` com:

- competência;
- contexto das quatro métricas;
- `destaques` somente quando a observação está no quartil inferior ou superior.

Os destaques usam linguagem descritiva, não julgamento universal. Exemplos:

- “A taxa observada está entre o quarto de menores taxas do segmento nesta competência.”
- “A operação observada por cotas em dia está entre o quarto de maiores do segmento; porte não é qualidade.”
- “O volume absoluto de contemplações no mês está entre o quarto de maiores do segmento; isso não mede chance nem velocidade individual.”

## Responsabilidade do frontend

O frontend deve **consumir** esta interpretação, não recalculá-la.

Pode:

- escolher hierarquia visual;
- escrever textos editoriais de conexão;
- mostrar setas, faixas e comparações;
- ordenar listas usando a ordem derivada já contratada;
- usar os destaques como “o que chama atenção”;
- explicar missingness em linguagem mais curta;
- construir busca, filtros, comparação lado a lado e navegação.

Não pode:

- somar posições/métricas;
- criar nota geral;
- decidir novos pesos;
- tratar maior porte como melhor;
- tratar menor taxa como “melhor administradora”;
- tratar mais contemplações absolutas como maior chance/velocidade;
- transformar índice ausente em zero;
- chamar `ordem_derivada` de ranking oficial do BCB;
- recalcular quartis/medianas de um subconjunto filtrado e apresentá-los como referência metodológica canônica.

## SEO e territórios

Esta camada não cria rotas, keywords, titles ou metas.

O frontend H12 pode usar os dados para responder intenções institucionais como comparação de administradoras, confiabilidade, ranking por critério, Banco Central e menor taxa observada. As páginas especializadas continuam donas de aquisição/finalidade (carro, imóvel, moto, serviços, carta contemplada) e a matéria de consórcio ou financiamento continua dona da comparação entre modalidades.

Nenhuma nova URL de “melhor-consorcio”, “ranking-administradoras”, “menor-taxa” ou perfil individual deve surgir automaticamente porque a interpretação existe.
