#!/bin/bash

# Define o modo de operação (master, worker, history)
SPARK_WORKLOAD=$1

echo "Iniciando Spark no modo: ${SPARK_WORKLOAD}"

# Limpa logs antigos se existirem
rm -f /opt/spark/logs/*

if [ "$SPARK_WORKLOAD" == "master" ]; then
  # Inicia o Master
  export SPARK_MASTER_PORT=7077
  export SPARK_MASTER_WEBUI_PORT=8080
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    --host spark-master \
    --port $SPARK_MASTER_PORT \
    --webui-port $SPARK_MASTER_WEBUI_PORT

elif [ "$SPARK_WORKLOAD" == "worker" ]; then
  # Inicia o Worker conectando no master
  # SPARK_MASTER_URL deve vir do ambiente ou default
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    ${SPARK_MASTER_URL:-spark://spark-master:7077} \
    --webui-port 8081

elif [ "$SPARK_WORKLOAD" == "history" ]; then
  # hadoop-aws e aws-java-sdk-bundle já vêm baked no $SPARK_HOME/jars/ via Dockerfile,
  # então o History Server consegue ler s3a://spark-events/ mesmo sem nenhum job ter rodado ainda.
  /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer

else
  echo "Erro: Modo desconhecido. Use 'master', 'worker' ou 'history'."
  exit 1
fi