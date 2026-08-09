# 🚀 CI/CD Setup - Data Platform

CI/CD pipeline set up for the **data_plataform** project with GitHub Actions.

## 📦 Structure

```
data_plataform/
├── .github/
│   ├── workflows/
│   │   ├── ci.yml           # CI pipeline (lint + tests)
│   │   ├── cd.yml           # CD pipeline (deploy)
│   │   └── pr-checks.yml    # Quality checks on PRs
│   └── README.md            # Full documentation
├── scripts/
│   └── lint.sh              # Script to run linters
├── pyproject.toml           # Linter configs (Ruff, Black, isort, pytest)
├── .flake8                  # Flake8 config
├── .sqlfluff                # SQLFluff config (SQL linting)
├── .pre-commit-config.yaml  # Pre-commit hooks
└── Makefile                 # Make commands (now with CI/CD)
```

## 🎯 How to Use

### Locally

```bash
cd data_plataform

# Install dev dependencies
make install-dev

# Run all linters
make lint

# Run and auto-fix
make lint-fix

# Run only Python linters
make lint-python

# Run only SQL linters
make lint-sql

# Format code
make format

# Run tests
make test

# Run tests with coverage
make test-cov

# Security scan
make security

# Full CI pipeline
make ci

# Clear cache
make clean-cache
```

### On GitHub

Workflows run automatically on:

- **CI Pipeline**: Push and PR to `main` and `devbranch`
- **CD Pipeline**: Push to `main` or `v*` tags
- **PR Checks**: PR opened/updated

## 🔧 Configured Workflows

### 1. CI Pipeline

- ✅ **Lint Python**: Ruff, Black, isort, Flake8
- ✅ **Lint SQL**: SQLFluff (dbt + postgres)
- ✅ **Tests**: pytest with coverage
- ✅ **DBT**: Model validation
- ✅ **Security**: Bandit, Safety

### 2. CD Pipeline

- ✅ **Docker Build**: DBAPI and Airflow
- ✅ **Deploy Staging**: Automatic deploy on main
- ✅ **Deploy Production**: Deploy on tags
- ✅ **DBT Deploy**: Deploy dbt models

### 3. PR Quality Checks

- ✅ **PR Title**: Conventional commits
- ✅ **Complexity**: Code analysis
- ✅ **Coverage**: Coverage report
- ✅ **SQL Changes**: SQL change analysis

## 🔍 Configured Linters

### Python

- **Ruff**: Super-fast linter (replaces Flake8, isort, etc.)
- **Black**: Code formatter (line-length: 100)
- **isort**: Import sorting
- **Flake8**: Style guide
- **mypy**: Type checking
- **Bandit**: Security
- **Safety**: Vulnerable dependencies

### SQL

- **SQLFluff**: SQL linter and formatter
  - Dialect: PostgreSQL
  - dbt template support
  - Keywords in UPPERCASE
  - Identifiers in lowercase

## ⚙️ Configuring GitHub Secrets

For CD to work, add these under:
**Settings → Secrets and variables → Actions**

```
DOCKER_USERNAME       # Docker Hub username
DOCKER_PASSWORD       # Docker Hub token
SLACK_WEBHOOK         # (Optional) Webhook for notifications
```

## 🚀 Creating Your First PR

```bash
# From the project root
cd data_plataform

# Create a branch
git checkout -b feature/test-ci

# Make changes
# ...

# Run CI locally before committing
make ci

# Commit and push
git add .
git commit -m "feat: add new feature"
git push origin feature/test-ci
```

The workflows will run automatically! 🎉

## 🎨 Customizing

### Changing lint rules

**Ruff**: Edit `[tool.ruff.lint]` in `pyproject.toml`

```toml
[tool.ruff.lint]
select = ["E", "F", "I", "B"]
ignore = ["E501"]
```

**SQLFluff**: Edit `[sqlfluff:rules]` in `.sqlfluff`

```ini
[sqlfluff:rules:L010]
capitalisation_policy = upper  # or lower
```

### Adding a new CI job

Edit `.github/workflows/ci.yml`:

```yaml
my-job:
  name: My Job
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - name: Run my task
      run: |
        cd data_plataform
        # your command here
```

## 📊 Badges

Add to the main README:

```markdown
![CI](https://github.com/your-user/your-repo/actions/workflows/ci.yml/badge.svg)
![CD](https://github.com/your-user/your-repo/actions/workflows/cd.yml/badge.svg)
```

## 🐛 Troubleshooting

### Error: "ruff: command not found"

```bash
make install-dev
```

### Workflow didn't run

1. Check that the workflows are under `data_plataform/.github/workflows/`
2. Go to **Actions** on GitHub
3. Check the logs

### SQLFluff failing

```bash
# Test locally
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

## 📚 Documentation

- Full documentation: [.github/README.md](.github/README.md)
- Make commands: `make help`
- GitHub Actions: [Actions Tab](https://github.com)

## 🎯 Commit Conventions

```
feat:     new feature
fix:      bug fix
docs:     documentation
style:    formatting
refactor: refactor
perf:     performance
test:     tests
build:    build
ci:       CI/CD
chore:    maintenance
```

---

**Configured and ready to use!** 🚀

For more details, see [.github/README.md](.github/README.md)
