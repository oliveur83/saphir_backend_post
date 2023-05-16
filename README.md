# Projet Python - Gestion de données avec PostgreSQL-SAPHIR

Ce projet consiste en la création d'une base de données pour ensuite insérer les données de deux types de sources (Météo France et Barani).
Ce projet Python vise donc a  lire des fichiers CSV, insérer leurs données dans une base de données PostgreSQL, créer la base de données puis relier ces deux fonctionnalités en traitant des requêtes HTTP avec des requêtes SQL.

# Structure du projet
Le projet est composé des fichiers suivants :

    lecture_fichier.py : Ce fichier lit les fichiers CSV et insère les données correspondantes dans la base de données PostgreSQL.
    bdd.py : Ce fichier crée la base de données PostgreSQL et insère des données dans certaines tables.
    main.py : Ce fichier récupère des requêtes HTTP, les traite avec des requêtes SQL et effectue les opérations nécessaires sur la base de données. puis les renvoie sous forme de json 
    
# Prérequis
Avant de commencer, assurez-vous d'avoir :

- Python 3.10.11(https://www.python.org/downloads/)
- flask 
- PostgreSQL 15(https://www.postgresql.org/download/)
- psycopg2

## Installation
1. Clonez ce référentiel GitHub :
```bash
git@github.com:oliveur83/saphir_backend_post.git
```
2.Accédez au répertoire du projet :
```
cd PROJETS
```
2.Installer du de la source baranie en telechargent ce dossier sur ce lien , a mettre a la racine de votre dossier 
```
https://drive.google.com/drive/folders/1qQdvzp377pOeD-5zxt6ftDVmol9POAZN?usp=sharing
```

3.Configurez les paramètres de connexion à la base de données PostgreSQL dans les fichiers lecture_fichier.py, bdd.py et main.py en remplaçant les valeurs suivantes :
```
    nom_de_la_base_de_données : Nom de la base de données que vous souhaitez utiliser ici (saphir).
    utilisateur : Nom d'utilisateur pour la connexion à la base de données.
    mot_de_passe : Mot de passe pour la connexion à la base de données.
    localhost : Adresse de l'hôte PostgreSQL.

```
4.Votre structure doit resemble a celle ci.
```
├── PROJETS
│   ├── Data_projet
│   ├── Datame_projet
│   └── projet_saphir
```
5. installer psycopg2 
```
pip install psycopg2 
```
#Utilisation

Exécutez main.py pour crée la base de donnée et inserer les donnée 
attention: 7 million de ligne sont a inserer c'est long.

#auteurs 

OLIVIER-OLIVEUR83
