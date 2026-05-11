.DEFAULT_GOAL := help
COMPOSE       := docker compose

# ── Help ────────────────────────────────────────────────────────────────────────
.PHONY: help
help: ## Mostra os comandos disponíveis
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ \
		{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ── Bootstrap (rodar uma vez) ────────────────────────────────────────────────────
.PHONY: init
init: ## Cria a rede e os volumes Docker compartilhados
	docker network create hadoopcluster  2>/dev/null || true
	docker volume  create dockervol      2>/dev/null || true
	docker volume  create spark-logs     2>/dev/null || true
	docker volume  create spark-ivy-cache 2>/dev/null || true
	@echo "✅  Rede hadoopcluster e volumes criados."

.PHONY: build
build: ## Builda as imagens customizadas (Spark · Airflow · dbapi)
	$(COMPOSE) -f spark/docker-compose.yml    build
	$(COMPOSE) -f airflow/docker-compose.yaml build
	$(COMPOSE) -f dbapi/docker-compose.yml    build
	@echo "✅  Imagens prontas."

.PHONY: setup
setup: init build ## Init + Build (first-time setup)

# ── Stack completa ───────────────────────────────────────────────────────────────
.PHONY: up
up: init ## Sobe toda a stack em ordem de dependência
	@echo "▶  [1/8] PostgreSQL..."
	$(COMPOSE) -f postgres/docker-compose.yml up -d --wait

	@echo "▶  [2/8] MinIO..."
	$(COMPOSE) -f minio/docker-compose.yml up -d --wait

	@echo "▶  [3/8] Hive Metastore..."
	$(COMPOSE) -f hive/docker-compose.yml up -d

	@echo "▶  [4/8] Unity Catalog OSS..."
	$(COMPOSE) -f unitycatalog/docker-compose.yml up -d

	@echo "▶  [5/8] Spark (aguardando master healthy)..."
	$(COMPOSE) -f spark/docker-compose.yml up -d --wait

	@echo "▶  [6/8] Trino..."
	$(COMPOSE) -f trino/docker-compose.yml up -d

	@echo "▶  [7/8] Airflow..."
	$(COMPOSE) -f airflow/docker-compose.yaml up -d

	@echo "▶  [8/8] dbapi..."
	$(COMPOSE) -f dbapi/docker-compose.yml up -d

	@echo ""
	@echo "✅  Stack completa. Acesso:"
	@echo "   MinIO Console      →  http://localhost:9001    (minioadmin / minioadmin)"
	@echo "   Spark UI           →  http://localhost:9090"
	@echo "   Spark History      →  http://localhost:18080"
	@echo "   Trino UI           →  http://localhost:8070"
	@echo "   Airflow UI         →  http://localhost:8181    (admin / admin)"
	@echo "   Unity Catalog API  →  http://localhost:8182/api/2.1/unity-catalog"

.PHONY: down
down: ## Para toda a stack (dados preservados nos volumes)
	@echo "⏹  Parando serviços..."
	-$(COMPOSE) -f dbapi/docker-compose.yml         down
	-$(COMPOSE) -f airflow/docker-compose.yaml      down
	-$(COMPOSE) -f trino/docker-compose.yml         down
	-$(COMPOSE) -f spark/docker-compose.yml         down
	-$(COMPOSE) -f unitycatalog/docker-compose.yml  down
	-$(COMPOSE) -f hive/docker-compose.yml          down
	-$(COMPOSE) -f minio/docker-compose.yml         down
	-$(COMPOSE) -f postgres/docker-compose.yml      down
	@echo "✅  Todos os serviços parados."

# ── Operações do dia a dia ───────────────────────────────────────────────────────
.PHONY: ps
ps: ## Lista containers rodando com status e portas
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | sort

.PHONY: logs
logs: ## Tail de logs — uso: make logs s=spark-master
	docker logs -f $(s)

.PHONY: restart
restart: ## Reinicia um container — uso: make restart s=spark-master
	docker restart $(s)

# ── Serviços individuais ─────────────────────────────────────────────────────────
.PHONY: up-postgres up-minio up-hive up-unitycatalog up-spark up-trino up-airflow up-dbapi

up-postgres: init ## Sobe apenas PostgreSQL
	$(COMPOSE) -f postgres/docker-compose.yml up -d --wait

up-minio: init ## Sobe apenas MinIO
	$(COMPOSE) -f minio/docker-compose.yml up -d --wait

up-hive: init ## Sobe apenas Hive Metastore
	$(COMPOSE) -f hive/docker-compose.yml up -d

up-unitycatalog: init ## Sobe apenas Unity Catalog OSS
	$(COMPOSE) -f unitycatalog/docker-compose.yml up -d

up-spark: init ## Builda e sobe apenas o cluster Spark
	$(COMPOSE) -f spark/docker-compose.yml build
	$(COMPOSE) -f spark/docker-compose.yml up -d --wait

up-trino: init ## Sobe apenas Trino
	$(COMPOSE) -f trino/docker-compose.yml up -d

up-airflow: init ## Sobe apenas Airflow
	$(COMPOSE) -f airflow/docker-compose.yaml up -d

up-dbapi: init ## Sobe apenas dbapi
	$(COMPOSE) -f dbapi/docker-compose.yml up -d

# ── Limpeza destrutiva ───────────────────────────────────────────────────────────
.PHONY: clean
clean: ## ⚠️  Remove containers E volumes — APAGA TODOS OS DADOS (5s para cancelar)
	@echo "⚠️  Isso vai destruir todos os dados. Ctrl+C para cancelar (5s)..."
	@sleep 5
	$(MAKE) down
	docker volume rm dockervol spark-logs spark-ivy-cache 2>/dev/null || true
	docker network rm hadoopcluster 2>/dev/null || true
	@echo "✅  Ambiente limpo."

# ── CI/CD e Qualidade de Código ──────────────────────────────────────────────
.PHONY: lint lint-fix lint-python lint-sql test security ci install-dev

install-dev: ## Instala dependências de desenvolvimento
	pip install ruff black isort flake8 mypy pytest pytest-cov sqlfluff sqlfluff-templater-dbt bandit safety pre-commit

lint: ## Executa todos os linters
	./scripts/lint.sh

lint-fix: ## Executa linters e corrige automaticamente
	./scripts/lint.sh --fix

lint-python: ## Executa apenas linters Python
	ruff check .
	black --check .
	isort --check-only .

lint-sql: ## Executa apenas linter SQL
	sqlfluff lint ../dbt/models/ --dialect postgres || true
	sqlfluff lint postgres/ --dialect postgres || true

format: ## Formata código Python e SQL
	ruff check . --fix
	black .
	isort .
	sqlfluff fix ../dbt/models/ --dialect postgres || true
	sqlfluff fix postgres/ --dialect postgres || true

test: ## Executa testes
	pytest dbapi/tests/ -v

test-cov: ## Executa testes com coverage
	pytest dbapi/tests/ --cov=dbapi --cov-report=html --cov-report=term

security: ## Executa varredura de segurança
	bandit -r . || true
	safety check || true

ci: lint test security ## Executa pipeline CI completo localmente
	@echo "✅  Pipeline CI concluído"

clean-cache: ## Remove arquivos de cache Python
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "✅  Cache limpo"
