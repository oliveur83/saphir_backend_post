# Saphir Backend
Ce projet consiste en la création d'une base de données pour ensuite insérer les données de deux types de sources (Météo France et Barani).

## commener 
### Prerequisites
Avant de commencer, assurez-vous d'avoir :

- Python 3.10.11
- flask 
- PostgreSQL 15

## Installation
1. Clonez le dépôt
```bash
git@github.com:oliveur83/saphir_backend_post.git
```

2.Installer du de la source baranie en telechargent ce dossier sur ce lien 
```
https://drive.google.com/drive/folders/1qQdvzp377pOeD-5zxt6ftDVmol9POAZN?usp=sharing
```

3.Dans PostgreSQL, créez une base de données nommée "saphir"

4. Modifiez les fichiers main.py et lecture_fichier.py en spécifiant votre nom d'utilisateur et votre mot de passe :
```
 host = "localhost"
    database = "saphir" 
    user = "votre_nom"
    password = "votre_mot_de_passe"
```


5. Exécutez le fichier main.py et cela devrait fonctionner.
