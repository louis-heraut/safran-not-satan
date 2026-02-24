.PHONY: help install install-prod install-service uninstall-service update run run-as-service download process upload clean hard-clean hard-clean-all status logs last-run stats check-env backup test

# Variables
PYTHON := python3
VENV := .python_env
BIN := $(VENV)/bin
PIP := $(BIN)/pip
PYTHON_VENV := $(BIN)/python

# Couleurs pour les messages
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Affiche cette aide
	@echo "$(GREEN)SAFRAN Fairy - Commandes disponibles$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'


install: ## Installe le projet (virtualenv + dépendances) - pour le dev
	@echo "$(GREEN)Installation de SAFRAN Fairy...$(NC)"
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "$(GREEN)✓ Installation terminée$(NC)"
	@echo "Pour activer l'environnement : source $(VENV)/bin/activate"

install-prod:
	@echo "$(GREEN)Configuration de SAFRAN Fairy pour la prod...$(NC)"
	sudo useradd --system --no-create-home --shell /usr/sbin/nologin safran-fairy 2>/dev/null || true
	sudo mkdir -p /var/lib/safran-fairy/{00_data-download,01_data-raw,02_data-split,03_data-convert,04_data-output}
	sudo chown -R safran-fairy:safran-fairy /var/lib/safran-fairy

install-service: install-prod
	@echo "$(GREEN)Installation du service systemd...$(NC)"
	sudo cp safran-sync.service /etc/systemd/system/
	sudo cp safran-sync.timer /etc/systemd/system/
	sudo systemctl daemon-reload
	sudo systemctl enable safran-sync.timer
	sudo systemctl start safran-sync.timer
	@echo "$(GREEN)✓ Service installé et activé$(NC)"

uninstall-service: ## Désinstalle le service systemd (nécessite sudo)
	@echo "$(YELLOW)Désinstallation du service systemd...$(NC)"
	sudo systemctl stop safran-sync.timer || true
	sudo systemctl disable safran-sync.timer || true
	sudo rm -f /etc/systemd/system/safran-sync.service
	sudo rm -f /etc/systemd/system/safran-sync.timer
	sudo systemctl daemon-reload
	sudo userdel safran-fairy 2>/dev/null || true
	@echo "$(GREEN)✓ Service désinstallé$(NC)"

update: ## Met à jour le projet depuis git
	@echo "$(GREEN)Mise à jour du projet...$(NC)"
	git pull
	$(PIP) install --upgrade -r requirements.txt
	@echo "$(GREEN)✓ Mise à jour terminée$(NC)"

run: ## Exécute le pipeline (dev, avec ton user)
	@echo "$(GREEN)Exécution du pipeline complet...$(NC)"
	$(PYTHON_VENV) main.py --all

run-as-service: ## Exécute comme le ferait le service systemd (nécessite sudo)
	@echo "$(GREEN)Exécution du pipeline complet par le service...$(NC)"
	sudo -u safran-fairy /opt/safran-fairy/.python_env/bin/python /opt/safran-fairy/main.py --all


download: ## Télécharge les nouvelles données uniquement
	@echo "$(GREEN)Téléchargement des données...$(NC)"
	$(PYTHON_VENV) main.py --download

process: ## Traite les données (decompress + split + convert + merge)
	@echo "$(GREEN)Traitement des données...$(NC)"
	$(PYTHON_VENV) main.py --process

upload: ## Upload sur Dataverse
	@echo "$(GREEN)Upload sur Dataverse...$(NC)"
	$(PYTHON_VENV) main.py --upload

clean: ## Nettoie les anciennes versions (local + Dataverse)
	@echo "$(GREEN)Nettoyage des anciennes versions...$(NC)"
	$(PYTHON_VENV) main.py --clean

hard-clean: ## Nettoie les fichiers temporaires (⚠️ destructif mais garde les outputs)
	@echo "$(YELLOW)Nettoyage des fichiers temporaires...$(NC)"
	rm -rf /var/lib/safran-fairy/01_data-raw/*
	rm -rf /var/lib/safran-fairy/02_data-split/*
	rm -rf /var/lib/safran-fairy/03_data-convert/*
	@echo "$(GREEN)✓ Nettoyage terminé$(NC)"

hard-clean-all: ## Nettoie TOUS les fichiers générés (⚠️ destructif)
	@echo "$(RED)⚠️  Suppression de TOUTES les données...$(NC)"
	@read -p "Êtes-vous sûr ? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		rm -rf /var/lib/safran-fairy/00_data-download/*; \
		rm -rf /var/lib/safran-fairy/01_data-raw/*; \
		rm -rf /var/lib/safran-fairy/02_data-split/*; \
		rm -rf /var/lib/safran-fairy/03_data-convert/*; \
		rm -rf /var/lib/safran-fairy/04_data-output/*; \
		echo "$(GREEN)✓ Nettoyage complet terminé$(NC)"; \
	fi

status: ## Affiche le statut du service systemd
	@echo "$(GREEN)Statut du service :$(NC)"
	@sudo systemctl status safran-sync.timer --no-pager || true
	@echo ""
	@echo "$(GREEN)Prochaines exécutions :$(NC)"
	@systemctl list-timers safran-sync.timer --no-pager || true

logs: ## Affiche les logs du service en temps réel
	@echo "$(GREEN)Logs en temps réel (Ctrl+C pour quitter) :$(NC)"
	sudo journalctl -u safran-sync.service -f

last-run: ## Affiche les logs de la dernière exécution
	@echo "$(GREEN)Logs de la dernière exécution :$(NC)"
	@sudo journalctl -u safran-sync.service --since "24 hours ago" --no-pager | tail -50

stats: ## Affiche des statistiques sur les données
	@echo "$(GREEN)Statistiques SAFRAN Fairy :$(NC)"
	@echo ""
	@echo "Espace disque :"
	@du -sh /var/lib/safran-fairy/0*_data-*/ 2>/dev/null || echo "  Aucune donnée"
	@echo ""
	@echo "Nombre de fichiers :"
	@echo "  Downloads    : $$(ls -1 /var/lib/safran-fairy/00_data-download/*.csv.gz 2>/dev/null | wc -l)"
	@echo "  Raw          : $$(ls -1 /var/lib/safran-fairy/01_data-raw/*.csv 2>/dev/null | wc -l)"
	@echo "  Split        : $$(ls -1 /var/lib/safran-fairy/02_data-split/*.parquet 2>/dev/null | wc -l)"
	@echo "  Converted    : $$(ls -1 /var/lib/safran-fairy/03_data-convert/*.nc 2>/dev/null | wc -l)"
	@echo "  Outputs      : $$(ls -1 /var/lib/safran-fairy/04_data-output/*.nc 2>/dev/null | wc -l)"
	@echo ""
	@echo "Dernière mise à jour :"
	@stat -c '%y %n' /var/lib/safran-fairy/04_data-output/*.nc 2>/dev/null | sort | tail -1 | awk '{print "  " $$1, $$2, $$4}' || echo "  Aucune donnée"
