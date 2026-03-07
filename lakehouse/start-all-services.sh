#!/bin/bash

# Função para verificar se um serviço está rodando
check_service() {
    local service_name=$1
    local service_status=$(ps aux | grep "$service_name" | grep -v grep)

    if [ -z "$service_status" ]; then
        echo "$service_name não está rodando. Verifique o serviço."
        return 1
    else
        echo "$service_name está rodando."
        return 0
    fi
}

# Inicia o JournalNode
echo "Iniciando o JournalNode..."
hdfs --daemon start journalnode
sleep 5  # Aguarda alguns segundos para o serviço iniciar

# Inicia o NameNode
echo "Iniciando o NameNode..."
hdfs --daemon start namenode
sleep 5  # Aguarda alguns segundos para o serviço iniciar

# Inicia o ZKServer
echo "Iniciando o ZKServer..."
zkServer.sh start
sleep 5  # Aguarda alguns segundos para o serviço iniciar

# Inicia o ZKFC (Zookeeper Failover Controller)
echo "Iniciando o ZKFC Controller..."
hdfs --daemon start zkfc
sleep 5  # Aguarda alguns segundos para o serviço iniciar

# Verifica se o ZKFC está rodando
echo "Verificando se o ZKFC Controller está rodando..."
check_service "zkfc"

echo "Todos os serviços foram iniciados com sucesso."

