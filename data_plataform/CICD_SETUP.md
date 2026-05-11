# 🚀 CI/CD Setup - Data Platform

Esteira de CI/CD configurada para o projeto **data_plataform** com GitHub Actions.

## 📦 Estrutura

```
data_plataform/
├── .github/
│   ├── workflows/
│   │   ├── ci.yml           # Pipeline de CI (lint + testes)
│   │   ├── cd.yml           # Pipeline de CD (deploy)
│   │   └── pr-checks.yml    # Checks de qualidade em PRs
│   └── README.md            # Documentação completa
├── scripts/
│   └── lint.sh              # Script para executar linters
├── pyproject.toml           # Configurações de linters (Ruff, Black, isort, pytest)
├── .flake8                  # Configurações Flake8
├── .sqlfluff                # Configurações SQLFluff (SQL linting)
├── .pre-commit-config.yaml  # Pre-commit hooks
└── Makefile                 # Comandos make (agora com CI/CD)
```

## 🎯 Como Usar

### Localmente

```bash
cd data_plataform

# Instalar dependências de desenvolvimento
make install-dev

# Executar todos os linters
make lint

# Executar e corrigir automaticamente
make lint-fix

# Executar apenas linters Python
make lint-python

# Executar apenas linters SQL
make lint-sql

# Formatar código
make format

# Executar testes
make test

# Executar testes com coverage
make test-cov

# Varredura de segurança
make security

# Pipeline CI completo
make ci

# Limpar cache
make clean-cache
```

### No GitHub

Os workflows executam automaticamente em:

- **CI Pipeline**: Push e PR para `main` e `devbranch`
- **CD Pipeline**: Push para `main` ou tags `v*`
- **PR Checks**: Abertura/atualização de PRs

## 🔧 Workflows Configurados

### 1. CI Pipeline

- ✅ **Lint Python**: Ruff, Black, isort, Flake8
- ✅ **Lint SQL**: SQLFluff (dbt + postgres)
- ✅ **Testes**: pytest com coverage
- ✅ **DBT**: Validação de modelos
- ✅ **Segurança**: Bandit, Safety

### 2. CD Pipeline

- ✅ **Docker Build**: DBAPI e Airflow
- ✅ **Deploy Staging**: Deploy automático na main
- ✅ **Deploy Production**: Deploy com tags
- ✅ **DBT Deploy**: Deploy de modelos dbt

### 3. PR Quality Checks

- ✅ **Título do PR**: Conventional commits
- ✅ **Complexidade**: Análise de código
- ✅ **Coverage**: Report de cobertura
- ✅ **SQL Changes**: Análise de mudanças SQL

## 🔍 Linters Configurados

### Python

- **Ruff**: Linter super rápido (substitui Flake8, isort, etc)
- **Black**: Formatador de código (line-length: 100)
- **isort**: Ordenação de imports
- **Flake8**: Guia de estilo
- **mypy**: Type checking
- **Bandit**: Segurança
- **Safety**: Dependências vulneráveis

### SQL

- **SQLFluff**: Linter e formatador SQL
  - Dialect: PostgreSQL
  - Suporte a templates dbt
  - Keywords em UPPERCASE
  - Identifiers em lowercase

## ⚙️ Configurar GitHub Secrets

Para o CD funcionar, adicione em:
**Settings → Secrets and variables → Actions**

```
DOCKER_USERNAME       # Usuário Docker Hub
DOCKER_PASSWORD       # Token Docker Hub
SLACK_WEBHOOK         # (Opcional) Webhook para notificações
```

## 🚀 Criar Primeiro PR

```bash
# Na raiz do projeto
cd data_plataform

# Criar branch
git checkout -b feature/test-ci

# Fazer mudanças
# ...

# Executar CI localmente antes do commit
make ci

# Commit e push
git add .
git commit -m "feat: adiciona nova funcionalidade"
git push origin feature/test-ci
```

Os workflows executarão automaticamente! 🎉

## 🎨 Customizar

### Alterar regras de lint

**Ruff**: Edite `[tool.ruff.lint]` em `pyproject.toml`

```toml
[tool.ruff.lint]
select = ["E", "F", "I", "B"]
ignore = ["E501"]
```

**SQLFluff**: Edite `[sqlfluff:rules]` em `.sqlfluff`

```ini
[sqlfluff:rules:L010]
capitalisation_policy = upper  # ou lower
```

### Adicionar novo job no CI

Edite `.github/workflows/ci.yml`:

```yaml
my-job:
  name: My Job
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - name: Run my task
      run: |
        cd data_plataform
        # seu comando aqui
```

## 📊 Badges

Adicione ao README principal:

```markdown
![CI](https://github.com/seu-usuario/seu-repo/actions/workflows/ci.yml/badge.svg)
![CD](https://github.com/seu-usuario/seu-repo/actions/workflows/cd.yml/badge.svg)
```

## 🐛 Troubleshooting

### Erro: "ruff: command not found"

```bash
make install-dev
```

### Workflow não executou

1. Verifique se os workflows estão em `data_plataform/.github/workflows/`
2. Vá em **Actions** no GitHub
3. Verifique os logs

### SQLFluff falhando

```bash
# Teste localmente
cd data_plataform
sqlfluff lint ../dbt/models/ --dialect postgres --verbose
```

### Pre-commit hooks

```bash
cd data_plataform
pip install pre-commit
pre-commit install
pre-commit run --all-files
```

## 📚 Documentação

- Documentação completa: [.github/README.md](.github/README.md)
- Comandos make: `make help`
- GitHub Actions: [Actions Tab](https://github.com)

## 🎯 Convenções de Commit

```
feat:     nova funcionalidade
fix:      correção de bug
docs:     documentação
style:    formatação
refactor: refatoração
perf:     performance
test:     testes
build:    build
ci:       CI/CD
chore:    manutenção
```

---

**Configurado e pronto para uso!** 🚀

Para mais detalhes, veja [.github/README.md](.github/README.md)
