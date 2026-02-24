# Installation


## Installation sur serveur Linux
### 1. Prérequis système
```bash
# Mise à jour
sudo apt update && sudo apt upgrade -y
# Dépendances système
sudo apt install -y \
    python3.10 \
    python3-pip \
    python3-venv \
    nco \
    git
```

### 2. Installation du projet
``` bash
# Cloner le projet
sudo mkdir -p /opt/safran-fairy
cd /opt/safran-fairy
git clone https://github.com/louis-heraut/safran-fairy.git .

# Installer le virtualenv et les dépendances Python
make install
```

### 3. Configuration
#### 3.1. env
```bash
# Copier et éditer le fichier d'environnement
cp env.dist .env
nano .env
```
Remplir avec vos paramètres pour la prod :
```bash
MODE=prod
CONFIG_FILE=config-prod.json
RDG_API_TOKEN=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

#### 3.2. config.json
```bash
# Copier et éditer le fichier de configuration
cp config.json.dist config-prod.json
nano config-prod.json
```
Remplir avec vos paramètres pour la prod :
```bash
"state_file": "/var/lib/safran-fairy/download_state.json",
"download_dir": "/var/lib/safran-fairy/00_data-download",
"raw_dir": "/var/lib/safran-fairy/01_data-raw",
"split_dir": "/var/lib/safran-fairy/02_data-split",
"convert_dir": "/var/lib/safran-fairy/03_data-convert",
"output_dir": "/var/lib/safran-fairy/04_data-output"
```

### 4. Installation prod et service systemd
``` bash
# Crée l'user système, les dossiers /var/lib avec les bons droits,
# installe et démarre le timer — tout en une commande
sudo make install-service
```

### 5. Test manuel
``` bash
# Tester le pipeline comme le ferait le service
make run-as-service

# Vérifier les données générées
ls -lh /var/lib/safran-fairy/04_data-output/
```

### 6. Vérification
``` bash
# Voir les prochaines exécutions planifiées
systemctl list-timers safran-sync.timer

# Tester une exécution manuelle via systemd
sudo systemctl start safran-sync.service

# Suivre les logs en temps réel
sudo journalctl -u safran-sync.service -f
# ou via make :
make logs
```


## Configuration avancée
### Changer l'heure d'exécution
Éditer le timer :
```bash
sudo systemctl edit safran-sync.timer
```
Ajouter :
```ini
[Timer]
OnCalendar=
OnCalendar=*-*-* 03:00:00
```
Recharger :
```bash
sudo systemctl daemon-reload
sudo systemctl restart safran-sync.timer
```

### Notifications par email
Installer postfix :
```bash
sudo apt install postfix mailutils
```
Modifier le service pour envoyer un email en cas d'erreur :
```bash
sudo systemctl edit safran-sync.service
```
Ajouter :
```ini
[Service]
OnFailure=status-email@%n.service
```

### Rotation des logs
Créer `/etc/logrotate.d/safran-fairy` :
```
/var/log/safran-fairy/*.log {
    daily
    rotate 30
    compress
    delaycompress
    notifempty
    create 0644 safran-user safran-user
    sharedscripts
}
```


## Monitoring
### Vérifier la santé du service
```bash
# Statut général
make status
# Dernière exécution
make last-run
# Logs des 24 dernières heures
sudo journalctl -u safran-sync.service --since "24 hours ago"
# Erreurs uniquement
sudo journalctl -u safran-sync.service -p err
```

### Métriques utiles
```bash
# Taille des données
du -sh /var/lib/safran-fairy/*/

# Nombre de fichiers traités
ls /var/lib/safran-fairy/04_data-output/*.nc | wc -l

# Dernière mise à jour
stat -c '%y' /var/lib/safran-fairy/04_data-output/*.nc | sort | tail -1
```


## Mise à jour
```bash
# Mettre à jour le code depuis git
cd /opt/safran-fairy
make update

# Le service utilisera automatiquement le nouveau code 
# à sa prochaine exécution (quotidienne à 02:00)
# OU tester immédiatement :
sudo systemctl start safran-sync.service
sudo journalctl -u safran-sync.service -f
```

**Note :** La mise à jour ne modifie pas vos fichiers de configuration (`.env`, `config.json`) ni vos données.


## Désinstallation
```bash
# Arrêter et désactiver le service
sudo systemctl stop safran-sync.timer
sudo systemctl disable safran-sync.timer
sudo systemctl stop safran-sync.service
# Supprimer les fichiers systemd
sudo rm /etc/systemd/system/safran-sync.service
sudo rm /etc/systemd/system/safran-sync.timer
sudo systemctl daemon-reload
# Supprimer le projet (attention : supprime toutes les données !)
sudo rm -rf /opt/safran-fairy
```


## Migration vers un nouveau serveur
```bash
# Sur l'ancien serveur : sauvegarder la config et l'état
tar czf safran-backup.tar.gz .env resources/download_state.json
# Sur le nouveau serveur : suivre l'installation normale puis restaurer
tar xzf safran-backup.tar.gz
```