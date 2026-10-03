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
# Shared volumes live on the dedicated F: disk (bind to $(CONTAINERS_DATA_DIR)/<name>), not in the
# Docker Desktop vhdx on L:. The directories are created by scripts/windows/mount-minio-f.ps1 (as root).
CONTAINERS_DATA_DIR := $(shell grep -E '^CONTAINERS_DATA_DIR=' $(SECRETS) 2>/dev/null | cut -d= -f2)
MINIO_DATA_DIR      := $(shell grep -E '^MINIO_DATA_DIR=' $(SECRETS) 2>/dev/null | cut -d= -f2)
F_VOLUMES := dockervol spark-logs spark-ivy-cache
# Every minio-f-backed bind source: the named volumes plus the MinIO data dir.
# Passed to scripts/stage-minio-f.sh so the list stays single-sourced here.
F_BIND_SRCS := $(foreach v,$(F_VOLUMES),$(CONTAINERS_DATA_DIR)/$(v)) $(MINIO_DATA_DIR)
WSL_DISTRO  ?= $(WSL_DISTRO_NAME)

.PHONY: init
init: stage-minio-f ## Cria a rede e os volumes Docker compartilhados (volumes bind no disco F:)
	docker network create hadoopcluster  2>/dev/null || true
	@test -n "$(CONTAINERS_DATA_DIR)" || { echo "ERROR: CONTAINERS_DATA_DIR missing in $(SECRETS)"; exit 1; }
	@for v in $(F_VOLUMES); do \
	  docker volume inspect $$v >/dev/null 2>&1 || \
	  docker volume create --driver local -o type=none -o o=bind -o device=$(CONTAINERS_DATA_DIR)/$$v $$v; \
	done
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
	$(COMPOSE) -f minio/docker-compose.yml         build
	$(COMPOSE) -f hive/docker-compose.yml          build
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
		envsubst < spark/conf/spark-defaults.conf.template > spark/conf/spark-defaults.conf
	@echo "✅  Configs geradas: spark-defaults.conf"

# ── MinIO data disk guard ────────────────────────────────────────────────────────
# MinIO data lives on a dedicated disk that WSL2 mounts at /mnt/wsl/minio-f
# (scripts/windows/mount-minio-f.ps1). If it is not mounted, Docker silently creates an
# empty directory there and MinIO starts with no buckets, so fail first.
MINIO_DATA_MOUNT := /mnt/wsl/minio-f

.PHONY: check-minio-disk
check-minio-disk: ## Fails if the MinIO data disk is not mounted in WSL2
	@mountpoint -q $(MINIO_DATA_MOUNT) || { \
	  echo "ERROR: $(MINIO_DATA_MOUNT) is not mounted, so MinIO would start with empty storage."; \
	  echo "Run scripts/windows/mount-minio-f.ps1 from Windows (administrator PowerShell) and retry."; \
	  echo "See README.md, Known Issues #9, if the container already started on the empty directory."; \
	  exit 1; }

# ── MinIO bind-mount staging (self-heal) ──────────────────────────────────────────
# Docker Desktop does not recreate its WSL bind-mount staging for the minio-f disk
# after a remount, so every minio-f-backed volume fails to mount until the staging
# re-exports are rebuilt. scripts/stage-minio-f.sh rebuilds them deterministically
# (hash = sha256 of the source path). Idempotent; runs as root via WSL interop.
# See README.md, Known Issues #9.
.PHONY: stage-minio-f
stage-minio-f: check-minio-disk ## Recria o staging de bind-mount do Docker Desktop p/ os volumes do disco minio-f
	@wsl.exe -d $(WSL_DISTRO) -u root -- bash $(CURDIR)/scripts/stage-minio-f.sh $(F_BIND_SRCS)

# Postgres data lives on a dedicated disk that WSL2 mounts at /mnt/wsl/postgres-ssd (scheduled task
# "MountPostgresSSD-WSL", L:\\WSLDisks\\postgres-data.vhdx). Without it Docker would create an
# empty directory and Postgres would initialise a brand-new, empty cluster, so fail first.
POSTGRES_DATA_MOUNT := /mnt/wsl/postgres-ssd

.PHONY: check-postgres-disk
check-postgres-disk: ## Fails if the Postgres data disk is not mounted in WSL2
	@mountpoint -q $(POSTGRES_DATA_MOUNT) || { \
	  echo "ERROR: $(POSTGRES_DATA_MOUNT) is not mounted, so Postgres would start with an empty cluster."; \
	  echo "Run the scheduled task \"MountPostgresSSD-WSL\" (or wsl --mount from an administrator PowerShell) and retry."; \
	  exit 1; }

# ── Stack completa ───────────────────────────────────────────────────────────────
.PHONY: up
up: check-minio-disk check-postgres-disk init generate-confs build ## Sobe toda a stack em ordem de dependência (generate-confs antes do build)
	@echo "▶  [1/7] PostgreSQL..."
	$(COMPOSE) -f postgres/docker-compose.yml up -d --wait

	@echo "▶  [2/7] MinIO (+ provisionamento de buckets via minio-init)..."
	@# `--wait` no arquivo inteiro quebraria aqui: minio-init é um job one-shot que
	@# termina (exit 0) de propósito, e o `--wait` do Compose trata qualquer container
	@# parado como falha, mesmo com exit code 0. Por isso esperamos só o "minio" (via
	@# --wait com o nome do serviço) e depois rodamos o "minio-init" em foreground —
	@# sem -d, `up` bloqueia até o container terminar e propaga o exit code real dele.
	$(COMPOSE) -f minio/docker-compose.yml up -d --wait minio
	$(COMPOSE) -f minio/docker-compose.yml up minio-init

	@echo "▶  [3/7] Hive Metastore..."
	$(COMPOSE) -f hive/docker-compose.yml up -d

	@echo "▶  [4/7] Spark (aguardando master healthy)..."
	$(COMPOSE) -f spark/docker-compose.yml up -d --wait

	@echo "▶  [5/7] Trino..."
	$(COMPOSE) -f trino/docker-compose.yml up -d

	@echo "▶  [6/7] Airflow..."
	$(COMPOSE) -f airflow/docker-compose.yaml up -d

	@echo "▶  [7/7] dbapi..."
	$(COMPOSE) -f dbapi/docker-compose.yml up -d

	@echo ""
	@echo "✅  Stack completa. Acesso:"
	@echo "   MinIO Console      →  http://localhost:9001    (minioadmin / minioadmin)"
	@echo "   Spark UI           →  http://localhost:9090"
	@echo "   Spark History      →  http://localhost:18080"
	@echo "   Trino UI           →  http://localhost:8070"
	@echo "   Airflow UI         →  http://localhost:8181    (admin / admin)"

.PHONY: down
down: ## Para e REMOVE os containers de toda a stack (dados preservados nos volumes nomeados)
	@echo "⏹  Parando e removendo containers..."
	-$(COMPOSE) -f dbapi/docker-compose.yml         down
	-$(COMPOSE) -f airflow/docker-compose.yaml      down
	-$(COMPOSE) -f trino/docker-compose.yml         down
	-$(COMPOSE) -f spark/docker-compose.yml         down
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
	-$(COMPOSE) -f hive/docker-compose.yml          stop
	-$(COMPOSE) -f minio/docker-compose.yml         stop
	-$(COMPOSE) -f postgres/docker-compose.yml      stop
	@echo "✅  Containers parados (preservados — use \`make start\` pra retomar)."

.PHONY: start
start: stage-minio-f check-postgres-disk ## Reinicia os containers parados com `make stop` (sem recriar) — uso: make start WORKERS=3
	@echo "▶  Reiniciando containers..."
	$(COMPOSE) -f postgres/docker-compose.yml      start
	$(COMPOSE) -f minio/docker-compose.yml         start
	$(COMPOSE) -f hive/docker-compose.yml          start
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
.PHONY: up-postgres up-minio up-hive up-spark up-trino up-airflow up-dbapi

up-postgres: check-postgres-disk init ## Sobe apenas PostgreSQL
	$(COMPOSE) -f postgres/docker-compose.yml up -d --wait

up-minio: check-minio-disk init ## Sobe apenas MinIO (+ provisionamento de buckets via minio-init)
	$(COMPOSE) -f minio/docker-compose.yml up -d --wait minio
	$(COMPOSE) -f minio/docker-compose.yml up minio-init

up-hive: init ## Builda e sobe apenas o Hive Metastore
	$(COMPOSE) -f hive/docker-compose.yml build
	$(COMPOSE) -f hive/docker-compose.yml up -d

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
