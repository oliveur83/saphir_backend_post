Saphir Backend

Ce projet consiste en la création d'une base de données pour ensuite insérer les données de deux types de sources (Météo France et Barani).
Commencer
Prérequis

Avant de commencer, assurez-vous d'avoir :

    Python 3.10.11
    Flask
    PostgreSQL 15

Installation

    Clonez le dépôt

bash

git clone git@github.com:oliveur83/saphir_backend_post.git

    Installez la source Barani en téléchargeant ce dossier à partir de ce lien

    Dans PostgreSQL, créez une base de données nommée "saphir"

    Modifiez les fichiers main.py et lecture_fichier.py en spécifiant votre nom d'utilisateur et votre mot de passe :

sql

 host = "localhost"
    database = "saphir" 
    user = "postgres"
    password = "toto"

    Exécutez le fichier main.py et cela devrait fonctionner.
