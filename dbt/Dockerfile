# Dockerfile
FROM python:3.10-slim-bullseye

# Previne arquivos .pyc e buffer de output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# Define o diretório de perfis para onde montaremos o volume
ENV DBT_PROFILES_DIR=/usr/app/dbt

# 1. Instalar dependências do sistema e Servidor SSH
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    openssh-server \
    sudo \
    libpq-dev \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Configurar o SSH
RUN mkdir /var/run/sshd
# Permite autenticação por senha (para estudo)
RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config

# 3. Criar o usuário 'dbt' e definir senha
# A senha será 'dbt' (usada no Airflow Connection)
RUN useradd -m -s /bin/bash dbt && \
    usermod -aG sudo dbt && \
    echo "dbt:dbt" | chpasswd && \
    echo "dbt ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# 4. Instalar o dbt e o adaptador Trino
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir dbt-core dbt-trino dbt-postgres

# 5. Preparar o diretório de trabalho
WORKDIR /usr/app/dbt

# Ajustar permissões iniciais (o volume sobrescreverá, mas é boa prática)
RUN chown -R dbt:dbt /usr/app/dbt

# 6. Expor a porta SSH
EXPOSE 22

# 7. Comando de inicialização
# Inicia o SSH em modo 'Daemon' (Foreground) para manter o container rodando
CMD ["/usr/sbin/sshd", "-D"]