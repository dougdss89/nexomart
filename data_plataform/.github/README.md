# CI/CD Pipeline Documentation

Este projeto possui uma esteira completa de CI/CD configurada com GitHub Actions.

## 📋 Workflows Disponíveis

### 1. CI Pipeline ([ci.yml](workflows/ci.yml))

Executado em: `push` e `pull_request` para branches `main` e `devbranch`

#### Jobs:

- **lint-python**: Validação de código Python
  - Ruff (linter rápido)
  - Black (formatação de código)
  - isort (ordenação de imports)
  - Flake8 (guia de estilo)

- **lint-sql**: Validação de código SQL
  - SQLFluff para modelos dbt
  - SQLFluff para scripts SQL

- **test-python**: Testes Python
  - pytest com coverage
  - Upload para Codecov

- **dbt-check**: Validação de modelos dbt
  - dbt parse

- **security-scan**: Varredura de segurança
  - Bandit (vulnerabilidades Python)
  - Safety (dependências vulneráveis)

### 2. CD Pipeline ([cd.yml](workflows/cd.yml))

Executado em: `push` para `main` ou tags `v*`

#### Jobs:

- **build-and-push-docker**: Build de imagens Docker
- **deploy-staging**: Deploy para staging (main branch)
- **deploy-production**: Deploy para produção (tags)
- **dbt-deploy**: Deploy de modelos dbt
- **notification**: Notificações de deploy

## 🚀 Como Usar

### Executar linters localmente

```bash
# Python linting
pip install ruff black isort flake8 mypy
ruff check .
black --check .
isort --check-only .
flake8 .

# SQL linting
pip install sqlfluff sqlfluff-templater-dbt
sqlfluff lint dbt/models/ --dialect postgres
sqlfluff fix dbt/models/ --dialect postgres  # auto-fix

# Security scanning
pip install bandit safety
bandit -r .
safety check
```

### Configurar secrets no GitHub

Para o workflow de CD funcionar, configure os seguintes secrets em:
`Settings > Secrets and variables > Actions`

```
DOCKER_USERNAME       # Usuário Docker Hub
DOCKER_PASSWORD       # Senha/Token Docker Hub
SLACK_WEBHOOK         # Webhook do Slack (opcional)
```

### Ambientes de Deploy

Configure os ambientes em: `Settings > Environments`

- **staging**: Ambiente de homologação
- **production**: Ambiente de produção

## 📝 Arquivos de Configuração

- [pyproject.toml](../pyproject.toml): Configurações de linters Python (ruff, black, isort, mypy, pytest)
- [.flake8](../.flake8): Configurações do Flake8
- [.sqlfluff](../.sqlfluff): Configurações do SQLFluff

## 🔧 Customização

### Adicionar novos jobs ao CI

Edite [.github/workflows/ci.yml](workflows/ci.yml):

```yaml
jobs:
  my-new-job:
    name: My New Job
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run my task
        run: echo "Hello"
```

### Modificar regras de lint

**Python (Ruff)**: Edite `[tool.ruff.lint]` em `pyproject.toml`

**SQL (SQLFluff)**: Edite `[sqlfluff:rules]` em `.sqlfluff`

### Habilitar/Desabilitar jobs específicos

Comente o job no workflow ou adicione condição:

```yaml
if: false  # Desabilita o job
```

## 📊 Badges de Status

Adicione ao seu README principal:

```markdown
![CI Pipeline](https://github.com/seu-usuario/seu-repo/actions/workflows/ci.yml/badge.svg)
![CD Pipeline](https://github.com/seu-usuario/seu-repo/actions/workflows/cd.yml/badge.svg)
[![codecov](https://codecov.io/gh/seu-usuario/seu-repo/branch/main/graph/badge.svg)](https://codecov.io/gh/seu-usuario/seu-repo)
```

## 🐛 Troubleshooting

### Workflow não está executando

1. Verifique se os arquivos estão em `.github/workflows/`
2. Verifique a sintaxe YAML
3. Veja os logs em `Actions` no GitHub

### SQLFluff falhando em modelos dbt

1. Configure `profiles_dir` corretamente em `.sqlfluff`
2. Certifique-se que o dbt está instalado
3. Use `sqlfluff lint --verbose` para debug

### Testes falhando

1. Verifique se as dependências estão corretas
2. Execute localmente: `pytest -v`
3. Verifique variáveis de ambiente necessárias

## 📚 Referências

- [GitHub Actions Docs](https://docs.github.com/en/actions)
- [Ruff Docs](https://docs.astral.sh/ruff/)
- [SQLFluff Docs](https://docs.sqlfluff.com/)
- [pytest Docs](https://docs.pytest.org/)
