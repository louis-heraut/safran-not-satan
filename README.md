```
                                                    _ ,       
 ݁₊                            . ݁       ⊹₊         ,- -      ⊹ . ݁     ₊ 
 ____  ₊ ݁.._   ⟡ _____  ____      _     _   _    _||_    _    '             
/ ___|₊ ⊹ / \   |  ___||  _ \. ݁₊ / \ ⟡ | \ | |  ' ||    < \, \\ ,._-_ '\\/\\ 
\___ \   / _ \  | |_   | |_) |  / _ \  |  \| |    || ⊹  /-|| ||  || ₊. || ;' 
 ___) | / ___ \ |  _|  |  _ < ⊹/ ___ \ | |\  |    |,   (( || ||  ||    ||/   
|____/ /_/   \_\|_|  ݁ .|_| \_\/_/   \_\|_| \_|  _-/     \/\\ \\  \\,   |/  
SAFRAN Fairy                                     . ݁. ݁       ⊹ ₊.      (      
                                                                       -_-
```

# SAFRAN Fairy

<!-- badges: start -->
[![Lifecycle: stable](https://img.shields.io/badge/lifecycle-stable-green)](https://lifecycle.r-lib.org/articles/stages.html)
![](https://img.shields.io/github/last-commit/louis-heraut/safran-fairy)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](code_of_conduct.md) 
<!-- badges: end -->

Pipeline automatisé de téléchargement, traitement et publication des données SAFRAN-ISBA-MODCOU (SIM2) au format NetCDF pour chaque variable disponible pour l'ensemble de la période de réanalyse depuis [meteo.data.gouv.fr](https://www.data.gouv.fr/datasets/donnees-changement-climatique-sim-quotidienne) vers [Recherche Data Gouv](https://doi.org/10.57745/BAZ12C).


## Raison d'être
Afin d'améliorer la réutilisabilité et en raison d'une accessibilité et d'une interopérabilité technique limitées des données dans leur format d'origine (CSV volumineux), ce projet automatise les étapes de :

1. **Téléchargement** des fichiers CSV depuis l'API Météo-France
2. **Décompression** des archives `.csv.gz`
3. **Découpage** par variable climatique
4. **Conversion** en NetCDF avec métadonnées
5. **Reconstruction** des séries temporelles (historical/previous/latest)
6. **Publication** sur un [dépôt de données](https://doi.org/10.57745/BAZ12C) de l'entrepôt Recherche Data Gouv.

*in dev* – Ce projet ajoute aux données d'origine l'évapotranspiration calculée selon la [méthode de Hargreaves](https://doi.org/10.13031/2013.26773) à partir des températures minimales et maximales de la réanalyse SAFRAN afin de continuer de répondre au besoin exprimé dans le cadre du projet [Explore2](https://entrepot.recherche.data.gouv.fr/dataverse/explore2).


### Stratégie de mise à jour
Trois fichiers NetCDF par variable pour optimiser la performance et la fraîcheur des données :

| Type | Description | Période couverte | Fréquence MAJ | Stabilité |
|------|-------------|------------------|---------------|-----------|
| **historical** | Chronique historique stable | 1958 → N-10 ans | Jamais | ✅ Stable |
| **previous** | Décennie en cours (mois complets) | N-10 ans → mois dernier | Mensuelle | 🔄 Mise à jour mensuelle |
| **latest** | Données les plus récentes | N-10 ans → aujourd'hui | Quotidienne | ⚡ Mise à jour quotidienne |


### Variables disponibles
26 variables climatiques quotidiennes sur grille Lambert II étendu (8 km) :

| Variable | Description | Unité |
|----------|-------------|-------|
| `PRENEI` | Précipitations solides | mm |
| `PRELIQ` | Précipitations liquides | mm |
| `T` | Température moyenne | °C |
| `TINF_H` | Température minimale | °C |
| `TSUP_H` | Température maximale | °C |
| `FF` | Vent moyen | m/s |
| `HU` | Humidité relative | % |
| ... | ... | ... |

Voir `resources/safran_variables.csv` pour la liste complète.


## Installation locale
### Prérequis
- Python 3.10+
- NCO (NetCDF Operators) : `sudo apt install nco`

### Installation
```bash
# Cloner le dépôt
git clone https://github.com/louis-heraut/safran-fairy.git
cd safran-fairy

# Installation
python -m venv .python_env
source .python_env/bin/activate
pip install -r requirements.txt
```

Voir [INSTALL.md](INSTALL.md) pour l'installation détaillée sur serveur Linux.


## Utilisation
### Exécution manuelle
```bash
# Pipeline complet
make run

# Étapes individuelles
make download    # Télécharger les nouvelles données
make process     # Traiter (décompresser, découper, convertir, fusionner)
make upload      # Publier sur Dataverse
make clean-old   # Nettoyer les anciennes versions
```

### Service systemd (production)
```bash
# Installation du service
sudo make install-service

# Vérifier le statut
sudo systemctl status safran-sync.timer
sudo journalctl -u safran-sync.service -f
```

Le service s'exécute quotidiennement à 02:00 UTC.

### Monitoring
```bash
# Logs en temps réel
make logs

# Statut du service
make status

# Dernière exécution
make last-run
```

### Architecture
```
safran_fairy/
├── download.py      # Téléchargement depuis meteo.data.gouv.fr
├── decompress.py    # Extraction des .csv.gz
├── split.py         # Découpage par variable
├── convert.py       # Conversion CSV → NetCDF
├── merge.py         # Fusion temporelle
├── upload.py        # Publication Dataverse
└── clean.py         # Nettoyage des anciennes versions
```

### Structure des données
```
00_download/     # Fichiers .csv.gz bruts téléchargés
01_raw/          # Fichiers .csv décompressés
02_split/        # Fichiers .parquet par variable
03_convert/      # Fichiers .nc individuels
04_output/       # Fichiers .nc fusionnés (historical/previous/latest)
```


## Contact
Maintenu par [Lou Heraut](mailto:louis.heraut@inrae.fr) ([INRAE](https://agriculture.gouv.fr/inrae-linstitut-national-de-recherche-pour-lagriculture-lalimentation-et-lenvironnement), [UR RiverLy](https://www.riverly.inrae.fr/), Villeurbanne, France)
