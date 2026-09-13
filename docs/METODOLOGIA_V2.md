# Comparador de Consórcios V2 — contrato metodológico

## Objetivo do produto

A V2 deve responder, em ordem, três perguntas do usuário:

1. **Quem é a administradora procurada e em quais segmentos há operação observada?**
2. **Quais sinais públicos existem para avaliar essa administradora, e quais são os limites desses sinais?**
3. **Como esses sinais se comparam aos de outras administradoras no mesmo recorte?**

A ferramenta é de **triagem baseada em dados públicos**, não um certificado de solvência, uma previsão de contemplação ou uma recomendação individual.

## Princípios obrigatórios

- A raiz cadastral de oito dígitos é a chave técnica primária.
- Cadastro atual, operação, reclamações, filiais e contexto econômico mantêm período e fonte próprios.
- `0`, `null`, não divulgado, não aplicável e sem vínculo são estados diferentes.
- Ausência de evidência não recebe nota neutra, bônus, penalidade arbitrária nem redistribuição de peso.
- Filiais e porte são informações descritivas; não constituem, por si, prova de confiabilidade.
- Oferta comercial, parceria ou remuneração não participam da avaliação institucional.
- Não existe `ranking geral` na primeira versão do contrato V2. A comparação é por dimensões verificáveis.

## Fonte canônica por métrica

### ConsorcioBD mensal

Para estoques, fluxos e taxa de administração por administradora/segmento, a fonte canônica é `Segmentos_Consolidados` na **competência mais recente selecionada pelo build**.

Arquivos de grupos não são somados ao consolidado. Eles podem enriquecer, quando compatíveis, apenas estatísticas cuja granularidade pertence aos grupos — atualmente prazo e valor médio do bem — sempre com cobertura explícita.

A inadimplência apresentada como participação é derivada por:

`inadimplentes / (cotas_em_dia + inadimplentes)`

somente quando numerador e denominador necessários estão disponíveis. A razão histórica `inadimplentes / cotas_em_dia` não é reutilizada com o rótulo de percentual do total.

### Ranking de reclamações do Banco Central

O índice oficial é preservado como publicado. O total de reclamações não é usado como numerador substituto do índice.

Se o arquivo não possuir coluna explícita de posição, `posicao_oficial` é `null`. A ordem física das linhas nunca é convertida em classificação do Banco Central.

Índice não divulgado permanece `null` com estado `nao_divulgado_pela_fonte`; isso não equivale a zero reclamações nem a reputação positiva.

## Identidade e portfólio

O catálogo contém todas as administradoras presentes no cadastro atual usado pelo build. Registros operacionais ou de reclamações sem raiz no cadastro atual são tratados como divergência temporal/orfandade e não são silenciosamente incorporados ao universo corrente.

O campo de portfólio significa **segmento com operação observada na competência**, não prova de que todos os planos comerciais daquele segmento estejam hoje disponíveis. O código 6 é rotulado conforme o dicionário auditado: `Serviços turísticos`.

## Comparabilidade

Cada administradora recebe um estado de cobertura, sem score geral:

- catálogo apenas;
- operação observada sem registro de reclamações vinculado;
- operação + registro de reclamações com índice não divulgado;
- operação + índice de reclamações divulgado.

As dimensões comparáveis são declaradas explicitamente. Uma dimensão ausente simplesmente não é comparada.

## Contratos públicos

- `administradoras.v2`: identidade, portfólio observado, operação, reclamações, presença informativa, cobertura e leitura limitada de confiabilidade.
- `produtos.v2`: uma linha canônica por raiz + competência + segmento.
- `rankings.v2`: dados do arquivo de reclamações sem posição inventada.
- `comparacoes.v2`: comparação por segmento e dimensões, sem ranking geral.
- `segmentos.v2`: contexto agregado, com derivados identificados como derivados.
- `ofertas.v2`: separado e vazio até existir fonte comercial com contrato próprio.

## Critérios de bloqueio do build

O build V2 falha quando:

- falta cadastro, filiais, ConsorcioBD mensal ou ranking de reclamações;
- o cadastro atual cai abaixo do limite de sanidade definido;
- não existe `Segmentos_Consolidados`;
- há chave duplicada raiz + competência + segmento no consolidado;
- a cobertura mensal cai abaixo do limite de sanidade;
- uma métrica operacional publicada não vem do consolidado canônico;
- saídas possuem raízes duplicadas ou contrato incompatível.

Esses limites são barreiras de integridade, não regras para congelar o universo real. Mudanças legítimas de estrutura/fonte exigem revisão explícita do contrato.
