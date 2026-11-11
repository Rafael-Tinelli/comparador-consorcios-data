# Proteção da branch main e publicação de dados V2

## Objetivo e fronteiras

- Código (`.github/workflows/`, `collectors/`, `transform/`, `scripts/`, `tests/`, `config/`, `hostgator/`, documentação): somente PR aprovado pelo check `validate-v2`.
- Dados: workflows numerados 01–04 e 06 podem continuar publicando snapshots e a release canônica sem PR individual, usando uma **GitHub App exclusiva** para bypass da regra da branch.
- A aplicação deve ter somente `Contents: Read and write` e `Metadata: Read`, instalada **exclusivamente neste repositório**, sem `Workflows: write`, `Administration: write` ou `Actions: write`.
- Cada workflow valida o índice do Git antes de fazer commit; só os caminhos explícitos no `scripts/guard_automated_data_commit.py` são admitidos.
- O bypass da GitHub App é *uma exceção para a identidade*, não uma permissão nativa restrita a pastas. O guarda no workflow complementa a regra, mas não elimina o risco de comprometimento das credenciais da App.

## Implementação no repositório

1. `05` é executado em **todo PR** (sem filtro de caminhos), para que o check obrigatório nunca fique pendente em mudanças fora dos filtros antigos.
2. Os workflows `01`–`04` e `06` geram token de App com `actions/create-github-app-token@v3`, usando o token do repositório apenas até a migração.
3. Antes de `git commit`, chamam o guarda com o identificador fixo daquele workflow.
4. O `GITHUB_TOKEN` padrão permanece como *fallback transitório* enquanto a regra da branch não está ativa. **Não ative a regra PR obrigatória antes de configurar e testar a App.**
5. Alteração na lista de caminhos, no workflow, no token ou nas regras de publicação deve passar por PR.

## Ativação segura — ordem obrigatória

### A. Merge e verificação do CI

- Integrar o PR desta preparação, depois de `05 · Validate Comparador V2 / validate-v2` ter passado.
- Verificar que `main` ainda está sem a nova regra, para que as coletas não sejam interrompidas prematuramente.

### B. Preparar identidade exclusiva

1. Registrar uma GitHub App na conta proprietária: exemplo de nome `comparador-consorcios-data-publisher`.
2. Configurar permissões *Repository permissions*: `Contents: Read and write`; `Metadata: Read`; demais apenas leitura/nenhuma. Não conceder `Workflows`, `Administration`, `Actions` ou permissões de organização.
3. Instalar a App **somente** em `Rafael-Tinelli/comparador-consorcios-data`.
4. Em *Settings → Secrets and variables → Actions* adicionar:
   - **Variable** `CONSORCIO_PUBLISH_APP_CLIENT_ID`: Client ID da App.
   - **Secret** `CONSORCIO_PUBLISH_APP_PRIVATE_KEY`: conteúdo da chave privada PEM. Nunca adicionar a chave ao Git nem aos logs.
5. Rodar manualmente `01 · Coleta BC Cadastro` **antes** de proteger a branch. Confirmar sucesso do token e push com a identidade da App.

### C. Ativar ruleset sobre a main

Em *Settings → Rules → Rulesets → New branch ruleset*:

- Nome: `V2 | main: PR para código + editor de dados`.
- Enforcement: **Active**; Target branches: `main` (ou *Default branch*).
- `Require a pull request before merging`: habilitado; inicialmente **0 aprovações obrigatórias** se há apenas um mantenedor (o GitHub não permite que a pessoa aprove seu próprio PR).
- `Require status checks to pass`: habilitar o check do job `validate-v2` do workflow `05 · Validate Comparador V2`; se oferecer, exigir branch atualizada antes do merge.
- `Block force pushes` e `Restrict deletions`: habilitados.
- Bypass list: **somente a GitHub App de publicação V2**, `Always allow`; não conceder bypass amplo à função `Write` ou a todas as Apps. Sem bypass administrativo rotineiro.
- Não habilitar `Restrict updates` sem avaliar os efeitos sobre merge de PR e bypass.

A configuração do ruleset é uma ação administrativa feita na interface GitHub e **não é aplicada por este PR**.

### D. Prova operacional após ativação

1. Criar PR com mudança inocente em `README.md`, verificar que o merge exige `validate-v2` e que push direto de usuário sem bypass é rejeitado.
2. Rodar `01` manualmente, confirmar que faz commit de `data/` via App com o ruleset ativo.
3. Rodar `06` manualmente e confirmar que gera/publica somente `data/dist-v2/global/*.json` e `data/source_state/*.json` autorizados.
4. Confirmar `data/dist-v2/global/meta.json` e `current-v2` no HostGator conforme processo independente de homologação.
5. Se houver erro de bypass, **não retirar a regra sem análise**; verificar token App, instalação, permissões e lista de bypass. Em contingência, pausar coletas até corrigir a identidade ou voltar temporariamente a publicação por PR monitorado.

## Limitação de segurança

GitHub não aplica `Require a pull request` apenas a arquivos de código num mesmo branch e permite push direto de outros arquivos: a regra incide sobre toda a branch. A exceção à GitHub App vale para a identidade; a restrição por caminho é uma verificação executada antes do commit pelos workflows confiáveis. Para garantias de isolamento mais forte, mover a publicação para branch/repositório separado ou serviço de publicação com permissões distintas.

## Evidência de configuração

A aprovação do workflow `05` comprova apenas o código da preparação. A proteção efetiva deve ser auditada em `Settings → Rulesets` ou `GET /repos/Rafael-Tinelli/comparador-consorcios-data/rulesets`. Não chamar este objetivo de concluído enquanto não houver ruleset **Active** e uma execução de coleta por App demonstrada.
