#!/bin/bash
# Script de synchronisation quotidienne des données météo

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/safran_downloader.py"
LOG_DIR="$SCRIPT_DIR/logs"
LOG_FILE="$LOG_DIR/sync_$(date +%Y%m%d_%H%M%S).log"

# Créer le dossier de logs s'il n'existe pas
mkdir -p "$LOG_DIR"

# Exécuter le script Python et logger la sortie
echo "======================================" | tee -a "$LOG_FILE"
echo "Synchronisation lancée le $(date)" | tee -a "$LOG_FILE"
echo "======================================" | tee -a "$LOG_FILE"

cd "$SCRIPT_DIR"
python3 "$PYTHON_SCRIPT" 2>&1 | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "Synchronisation terminée le $(date)" | tee -a "$LOG_FILE"

# Garder uniquement les 30 derniers logs
find "$LOG_DIR" -name "sync_*.log" -type f -mtime +30 -delete

exit 0
