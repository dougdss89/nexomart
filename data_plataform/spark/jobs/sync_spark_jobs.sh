#!/bin/bash
# Sincroniza pastas específicas da pasta development com a pasta local de jobs,
# e em seguida sincroniza tudo para o volume compartilhado (dockervol).
# Apenas arquivos diferentes ou novos são sincronizados.

ORIGEM="/opt/spark/development/"
DESTINO="/opt/spark/jobs"
DESTINO_VOL="/dockervol/spark/jobs"
LOG_FILE="/opt/spark/synclog/rsync_jobs.log"
PASTAS=("raw_layer_extract_scripts" "landing" "silver_layer_advworks_scripts")

# Verifica se o destino primário existe
if [ ! -d "$DESTINO" ]; then
  echo "$(date '+%Y-%m-%d %H:%M:%S') - ERRO: Pasta destino não existe: $DESTINO" >> "$LOG_FILE"
  exit 1
fi

# Verifica se o destino do volume existe (se não, cria)
if [ ! -d "$DESTINO_VOL" ]; then
  mkdir -p "$DESTINO_VOL"
  echo "$(date '+%Y-%m-%d %H:%M:%S') - INFO: Pasta destino do volume criada: $DESTINO_VOL" >> "$LOG_FILE"
fi

# 1ª ETAPA: Sincroniza as pastas específicas do development para a pasta local de jobs
for PASTA in "${PASTAS[@]}"; do
  SRC="$ORIGEM$PASTA/"
  DST="$DESTINO/$PASTA/"
  
  # Verifica se a pasta de origem existe
  if [ -d "$SRC" ]; then
    rsync -av --checksum --log-file="$LOG_FILE" "$SRC" "$DST"
    
    # Verifica o código de saída do rsync
    if [ $? -eq 0 ]; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Sincronização concluída com sucesso para $PASTA." >> "$LOG_FILE"
    else
      echo "$(date '+%Y-%m-%d %H:%M:%S') - ERRO: Problema ao sincronizar a pasta $PASTA." >> "$LOG_FILE"
    fi
  else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ERRO: Pasta origem não existe: $SRC" >> "$LOG_FILE"
  fi
done

# 2ª ETAPA: Sincroniza a pasta local de jobs atualizada para o volume do Docker
echo "$(date '+%Y-%m-%d %H:%M:%S') - Iniciando espelhamento para o volume: $DESTINO_VOL" >> "$LOG_FILE"

rsync -av --checksum --log-file="$LOG_FILE" "$DESTINO/" "$DESTINO_VOL/"

if [ $? -eq 0 ]; then
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Espelhamento para o volume dockervol concluído com sucesso." >> "$LOG_FILE"
else
  echo "$(date '+%Y-%m-%d %H:%M:%S') - ERRO: Problema ao espelhar para o volume dockervol." >> "$LOG_FILE"
fi
