# Use uma imagem base com Python
FROM python:3.10-bullseye

# Defina variáveis de ambiente necessárias para o Airflow
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_VERSION=2.9.1
ENV AIRFLOW_USER_HOME=/home/airflow

# Instale dependências necessárias
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       libssl-dev \
       libffi-dev \
       libblas-dev \
       liblapack-dev \
       libpq-dev \
       curl \
       netcat \
       libjpeg-dev \
       freetds-dev \
       libkrb5-dev \
       libsasl2-dev \
       libldap2-dev \
       libgmp-dev \
       python3-dev \
       unixodbc-dev \
       openjdk-11-jdk \
       ant \
       git \
       && apt-get clean \
       && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# Crie o diretório para o Airflow
RUN mkdir -p ${AIRFLOW_HOME}
WORKDIR ${AIRFLOW_HOME}

# Instale o Apache Airflow
RUN pip3.10 install apache-airflow[celery,postgres]==${AIRFLOW_VERSION}

# Instale os requisitos
COPY requirements/requirements.txt .
RUN pip3.10 install -r requirements.txt

# Permissão para DAGs
COPY --chown=airflow:root ./dags /usr/airflow/dags

# Crie um usuário para o Airflow
RUN useradd -ms /bin/bash airflow

# Ajuste as permissões das pastas para o usuário airflow
RUN chown -R airflow:airflow ${AIRFLOW_HOME} ${AIRFLOW_USER_HOME}

# Defina o diretório home do usuário airflow
WORKDIR ${AIRFLOW_USER_HOME}

# Copie arquivos necessários para o contêiner
COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Crie volume para compartilhamento de dados
VOLUME /spark_airflow

# Ajuste as permissões do volume para todos os usuários
# RUN chmod -R 777 /spark_airflow

# Defina o usuário para a execução
USER airflow

# Exponha as portas necessárias
EXPOSE 8081 8793 5555 8080 6379

# Comando padrão de entrada
ENTRYPOINT ["/entrypoint.sh"]
