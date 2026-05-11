#!/bin/bash

# Script para executar todos os linters localmente
# Uso: ./scripts/lint.sh [--fix]

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

FIX_MODE=false
if [[ "$1" == "--fix" ]]; then
    FIX_MODE=true
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}🔍 Executando Linters${NC}"
echo -e "${GREEN}========================================${NC}"

# Função para verificar se comando existe
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Instalar dependências se necessário
if ! command_exists ruff || ! command_exists black || ! command_exists sqlfluff; then
    echo -e "${YELLOW}⚠️  Instalando dependências de lint...${NC}"
    pip install ruff black isort flake8 mypy sqlfluff sqlfluff-templater-dbt bandit safety
fi

# Python Linting
echo -e "\n${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}🐍 Python Linting${NC}"
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

echo -e "\n${GREEN}▶ Ruff${NC}"
if [ "$FIX_MODE" = true ]; then
    ruff check . --fix || true
else
    ruff check . || true
fi

echo -e "\n${GREEN}▶ Black${NC}"
if [ "$FIX_MODE" = true ]; then
    black . || true
else
    black --check --diff . || true
fi

echo -e "\n${GREEN}▶ isort${NC}"
if [ "$FIX_MODE" = true ]; then
    isort . || true
else
    isort --check-only --diff . || true
fi

echo -e "\n${GREEN}▶ Flake8${NC}"
flake8 . || true

# SQL Linting
echo -e "\n${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}🗄️  SQL Linting${NC}"
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if [ -d "dbt/models" ]; then
    echo -e "\n${GREEN}▶ SQLFluff (dbt models)${NC}"
    if [ "$FIX_MODE" = true ]; then
        sqlfluff fix dbt/models/ --dialect postgres || true
    else
        sqlfluff lint dbt/models/ --dialect postgres || true
    fi
fi

if [ -d "data_plataform/postgres" ]; then
    echo -e "\n${GREEN}▶ SQLFluff (postgres scripts)${NC}"
    if [ "$FIX_MODE" = true ]; then
        sqlfluff fix data_plataform/postgres/ --dialect postgres || true
    else
        sqlfluff lint data_plataform/postgres/ --dialect postgres || true
    fi
fi

# Security Scanning
echo -e "\n${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}🔒 Security Scanning${NC}"
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

echo -e "\n${GREEN}▶ Bandit${NC}"
bandit -r . -f screen || true

echo -e "\n${GREEN}▶ Safety${NC}"
safety check || true

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}✅ Linting concluído!${NC}"
echo -e "${GREEN}========================================${NC}"

if [ "$FIX_MODE" = true ]; then
    echo -e "${YELLOW}💡 Arquivos foram automaticamente corrigidos onde possível${NC}"
else
    echo -e "${YELLOW}💡 Execute com --fix para corrigir automaticamente${NC}"
fi
