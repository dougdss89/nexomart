# CI/CD Pipeline Documentation

This project has a full CI/CD pipeline set up with GitHub Actions.

## 📋 Available Workflows

### 1. CI Pipeline ([ci.yml](workflows/ci.yml))

Runs on: `push` and `pull_request` to the `main` and `devbranch` branches

#### Jobs:

- **lint-python**: Python code validation
  - Ruff (fast linter)
  - Black (code formatting)
  - isort (import sorting)
  - Flake8 (style guide)

- **lint-sql**: SQL code validation
  - SQLFluff for dbt models
  - SQLFluff for SQL scripts

- **test-python**: Python tests
  - pytest with coverage
  - Upload to Codecov

- **dbt-check**: dbt model validation
  - dbt parse

- **security-scan**: Security scan
  - Bandit (Python vulnerabilities)
  - Safety (vulnerable dependencies)

### 2. CD Pipeline ([cd.yml](workflows/cd.yml))

Runs on: `push` to `main` or `v*` tags

#### Jobs:

- **build-and-push-docker**: Build Docker images
- **deploy-staging**: Deploy to staging (main branch)
- **deploy-production**: Deploy to production (tags)
- **dbt-deploy**: Deploy dbt models
- **notification**: Deploy notifications

## 🚀 How to Use

### Running linters locally

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

### Configuring GitHub secrets

For the CD workflow to work, configure the following secrets under:
`Settings > Secrets and variables > Actions`

```
DOCKER_USERNAME       # Docker Hub username
DOCKER_PASSWORD       # Docker Hub password/token
SLACK_WEBHOOK         # Slack webhook (optional)
```

### Deploy Environments

Configure environments under: `Settings > Environments`

- **staging**: Staging environment
- **production**: Production environment

## 📝 Configuration Files

- [pyproject.toml](../pyproject.toml): Python linter configs (ruff, black, isort, mypy, pytest)
- [.flake8](../.flake8): Flake8 config
- [.sqlfluff](../.sqlfluff): SQLFluff config

## 🔧 Customization

### Adding new jobs to CI

Edit [.github/workflows/ci.yml](workflows/ci.yml):

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

### Changing lint rules

**Python (Ruff)**: Edit `[tool.ruff.lint]` in `pyproject.toml`

**SQL (SQLFluff)**: Edit `[sqlfluff:rules]` in `.sqlfluff`

### Enabling/disabling specific jobs

Comment out the job in the workflow, or add a condition:

```yaml
if: false  # Disables the job
```

## 📊 Status Badges

Add to your main README:

```markdown
![CI Pipeline](https://github.com/your-user/your-repo/actions/workflows/ci.yml/badge.svg)
![CD Pipeline](https://github.com/your-user/your-repo/actions/workflows/cd.yml/badge.svg)
[![codecov](https://codecov.io/gh/your-user/your-repo/branch/main/graph/badge.svg)](https://codecov.io/gh/your-user/your-repo)
```

## 🐛 Troubleshooting

### Workflow isn't running

1. Check that the files are under `.github/workflows/`
2. Check the YAML syntax
3. Check the logs under `Actions` on GitHub

### SQLFluff failing on dbt models

1. Configure `profiles_dir` correctly in `.sqlfluff`
2. Make sure dbt is installed
3. Use `sqlfluff lint --verbose` to debug

### Tests failing

1. Check that dependencies are correct
2. Run locally: `pytest -v`
3. Check required environment variables

## 📚 References

- [GitHub Actions Docs](https://docs.github.com/en/actions)
- [Ruff Docs](https://docs.astral.sh/ruff/)
- [SQLFluff Docs](https://docs.sqlfluff.com/)
- [pytest Docs](https://docs.pytest.org/)
