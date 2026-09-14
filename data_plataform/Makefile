.DEFAULT_GOAL := help
SECRETS       := configs/secrets.env
COMPOSE       := docker compose --env-file $(SECRETS)
WORKERS       ?= 1

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
	docker volume  create spark-jobs     2>/dev/null || true
	@# astro (uid 50000, gid 50000) precisa escrever em /dockervol/airflow-logs
	@# (AIRFLOW__LOGGING__BASE_LOG_FOLDER); o volume nasce root:root 755, então
	@# criamos a pasta com o dono certo e liberamos escrita de grupo p/ futuras subpastas.
	docker run --rm -v dockervol:/dockervol busybox sh -c \
		"mkdir -p /dockervol/airflow-logs && chown -R 50000:50000 /dockervol/airflow-logs && chmod 775 /dockervol"
	@# spark-ivy-cache também nasce root:root 755. Tanto o processo spark (uid 1000,
	@# em spark-master/worker) quanto astro (uid 50000, quando o SparkSubmitOperator do
	@# Airflow roda spark-submit como driver a partir do airflow-worker) precisam escrever
	@# ali para resolver spark.jars.packages via Ivy — sem grupo em comum, liberamos 777.
	docker run --rm -v spark-ivy-cache:/ivy busybox chmod 777 /ivy
	@echo "✅  Rede hadoopcluster e volumes criados."

.PHONY: build
build: ## Builda as imagens customizadas (Hive · Spark · Airflow · dbapi)
	$(COMPOSE) -f hive/docker-compose.yml          build
	$(COMPOSE) -f unitycatalog/docker-compose.yml  build
	$(COMPOSE) -f spark/docker-compose.yml         build
	$(COMPOSE) -f airflow/docker-compose.yaml      build
	$(COMPOSE) -f dbapi/docker-compose.yml         build
	@echo "✅  Imagens prontas."

.PHONY: setup
setup: init build ## Init + Build (first-time setup)

# ── Geração de configs a partir de templates ────────────────────────────────────
.PHONY: generate-confs
generate-confs: ## Gera arquivos de config com credenciais a partir dos templates
	@test -f $(SECRETS) || (echo "❌  $(SECRETS) não encontrado. Copie secrets.env.example e preencha." && exit 1)
	@export $$(grep -v '^#' $(SECRETS) | xargs) && \
		envsubst < spark/conf/spark-defaults.conf.template > spark/conf/spark-defaults.conf && \
		envsubst < unitycatalog/conf/hibernate.properties.template > unitycatalog/conf/hibernate.properties
	@echo "✅  Configs geradas: spark-defaults.conf, hibernate.properties"

# ── Stack completa ───────────────────────────────────────────────────────────────
.PHONY: up
up: init generate-confs build ## Sobe toda a stack em ordem de dependência (generate-confs antes do build)
	@echo "▶  [1/8] PostgreSQL..."
	$(COMPOSE) -f postgres/docker-compose.yml up -d --wait

	@echo "▶  [2/8] MinIO (+ provisionamento de buckets via minio-init)..."
	@# `--wait` no arquivo inteiro quebraria aqui: minio-init é um job one-shot que
	@# termina (exit 0) de propósito, e o `--wait` do Compose trata qualquer container
	@# parado como falha, mesmo com exit code 0. Por isso esperamos só o "minio" (via
	@# --wait com o nome do serviço) e depois rodamos o "minio-init" em foreground —
	@# sem -d, `up` bloqueia até o container terminar e propaga o exit code real dele.
	$(COMPOSE) -f minio/docker-compose.yml up -d --wait minio
	$(COMPOSE) -f minio/docker-compose.yml up minio-init

	@echo "▶  [3/8] Hive Metastore..."
	$(COMPOSE) -f hive/docker-compose.yml up -d

	@echo "▶  [4/8] Unity Catalog OSS..."
	$(COMPOSE) -f unitycatalog/docker-compose.yml up -d --wait

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
down: ## Para e REMOVE os containers de toda a stack (dados preservados nos volumes nomeados)
	@echo "⏹  Parando e removendo containers..."
	-$(COMPOSE) -f dbapi/docker-compose.yml         down
	-$(COMPOSE) -f airflow/docker-compose.yaml      down
	-$(COMPOSE) -f trino/docker-compose.yml         down
	-$(COMPOSE) -f spark/docker-compose.yml         down
	-$(COMPOSE) -f unitycatalog/docker-compose.yml  down
	-$(COMPOSE) -f hive/docker-compose.yml          down
	-$(COMPOSE) -f minio/docker-compose.yml         down
	-$(COMPOSE) -f postgres/docker-compose.yml      down
	@echo "✅  Todos os containers removidos (volumes intactos — use \`make up\` para recriar)."

.PHONY: stop
stop: ## Só PARA os containers (sem remover) — mais rápido pra retomar depois com `make start`
	@echo "⏸  Parando containers (sem remover)..."
	-$(COMPOSE) -f dbapi/docker-compose.yml         stop
	-$(COMPOSE) -f airflow/docker-compose.yaml      stop
	-$(COMPOSE) -f trino/docker-compose.yml         stop
	-$(COMPOSE) -f spark/docker-compose.yml         stop
	-$(COMPOSE) -f unitycatalog/docker-compose.yml  stop
	-$(COMPOSE) -f hive/docker-compose.yml          stop
	-$(COMPOSE) -f minio/docker-compose.yml         stop
	-$(COMPOSE) -f postgres/docker-compose.yml      stop
	@echo "✅  Containers parados (preservados — use \`make start\` pra retomar)."

.PHONY: start
start: ## Reinicia os containers parados com `make stop` (sem recriar) — uso: make start WORKERS=3
	@echo "▶  Reiniciando containers..."
	$(COMPOSE) -f postgres/docker-compose.yml      start
	$(COMPOSE) -f minio/docker-compose.yml         start
	$(COMPOSE) -f hive/docker-compose.yml          start
	$(COMPOSE) -f unitycatalog/docker-compose.yml  start
	@# `start` só reinicia containers que já existem — não cria réplicas novas. Pra
	@# poder pedir WORKERS=N aqui (inclusive N > quantos existiam quando parou), usamos
	@# `up -d --scale` em vez de `start`: se nada mudou de config, ele só inicia os
	@# containers parados (mesmo efeito de `start`); se WORKERS mudou, ele cria/remove
	@# réplicas pra bater com o número pedido.
	$(COMPOSE) -f spark/docker-compose.yml up -d --scale spark-worker=$(WORKERS)
	$(COMPOSE) -f trino/docker-compose.yml         start
	$(COMPOSE) -f airflow/docker-compose.yaml      start
	$(COMPOSE) -f dbapi/docker-compose.yml         start
	@echo "✅  Stack reiniciada (spark-worker: $(WORKERS) réplica(s))."

.PHONY: scale-spark
scale-spark: ## Escala spark-worker sem mexer no resto da stack — uso: make scale-spark WORKERS=3
	$(COMPOSE) -f spark/docker-compose.yml up -d --scale spark-worker=$(WORKERS)
	@echo "✅  spark-worker escalado para $(WORKERS) réplica(s)."

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

up-minio: init ## Sobe apenas MinIO (+ provisionamento de buckets via minio-init)
	$(COMPOSE) -f minio/docker-compose.yml up -d --wait minio
	$(COMPOSE) -f minio/docker-compose.yml up minio-init

up-hive: init ## Builda e sobe apenas o Hive Metastore
	$(COMPOSE) -f hive/docker-compose.yml build
	$(COMPOSE) -f hive/docker-compose.yml up -d

up-unitycatalog: init ## Sobe apenas Unity Catalog OSS
	$(COMPOSE) -f unitycatalog/docker-compose.yml up -d --wait

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
	sqlfluff lint dbt/models/ --dialect postgres || true
	sqlfluff lint postgres/ --dialect postgres || true

format: ## Formata código Python e SQL
	ruff check . --fix
	black .
	isort .
	sqlfluff fix dbt/models/ --dialect postgres || true
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
