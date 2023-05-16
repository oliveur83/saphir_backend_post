# 4 table 2 par 2
import psycopg2
from flask import Flask, request
import os 
from lecture_fichier import barani_1_cat,barani_2_cat,meteo_fr_1_cat,meteo_fr_2_cat
from BDD import creation_bdd

import json 

def main():
    """
    Fonction principale
    """

    # Connexion à la base de données
    conn = connect_to_database()
    #creation de la base de donnée
    creation_bdd()


    """ 
    lecture fichier csv 
    """
    # chemin pour baranie
    chemin_actuel = os.getcwd()
    chemin_dossier_baranie = chemin_actuel+'/projets/data_projet'
    
    # chemin pour meteo france 
    chemin_dossier_meteo_wind = chemin_actuel+'/projets/datame_saphir/Wind2m_Rain_Temp'
    chemin_dossier_meteo_10wind = chemin_actuel+'/projets/datame_saphir/Wind10m_inso_Relamoist_statpress'
    
    # Parcourez les fichiers du dossier baranie
    for nom_fichier in os.listdir(chemin_dossier_baranie):
        # Si le chemin correspond à un dossier type 
        if nom_fichier[4]=='S' and nom_fichier[5]=='W':
            print(nom_fichier)
            #barani_1_cat(chemin_dossier_baranie,nom_fichier,conn)  
        # Vérifiez si le nom du dossier correspond à celui que vous recherchez
        elif nom_fichier[4]=='S' and nom_fichier[5]=='H':
            print(nom_fichier)
           # barani_2_cat(chemin_dossier_baranie,nom_fichier,conn)
    print("baranie reussi ")
    for nom_fichier in os.listdir(chemin_dossier_meteo_wind):
        # Si le chemin correspond à un dossier
        if nom_fichier[0]=='C':
            print(nom_fichier)
           # meteo_fr_1_cat(chemin_dossier_meteo_wind,nom_fichier)
    for nom_fichier in os.listdir(chemin_dossier_meteo_10wind):
        # Si le chemin correspond à un dossier
        if nom_fichier[0]=='C':
            print(nom_fichier)
          #  meteo_fr_2_cat(chemin_dossier_meteo_10wind,nom_fichier)
    print("meteo france reussi")


app = Flask(__name__)

def connect_to_database():

    """
    Fonction pour se connecter à la base de données
    """
    host = "localhost"
    database = "saphir" 
    user = "postgres"
    password = "toto"
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    return conn

@app.route('/datatypes')
def get_datatypes():
    """
    Route pour récupérer les types de données
    """
    source = request.args.get('source')
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute(f"""SELECT json_agg(td.*) 
        FROM typedata td  
        WHERE td.id_source = '{source}';
    """)
    data = cur.fetchall()
    response_data = json.loads(json.dumps(data[0][0])) if data else []
    resultat = {
        "code": 200,
        "message": "success",
        "data": response_data
    }
    return json.dumps(resultat)

@app.route('/station')
def get_station():
    """
    Route pour récupérer les stations
    """
    source = request.args.get('source')
    datatype = request.args.get('datatype')
    conn = connect_to_database()
    cur = conn.cursor()
    if datatype is None:
        cur.execute(f"""SELECT json_agg(s) 
            FROM station s 
            JOIN source src ON src.id_source = s.id_source 
            WHERE s.id_source = '{source}';
        """)
    else:
        cur.execute(f"""SELECT json_agg(distinct s.*) 
            FROM station s 
            INNER JOIN typedata td ON s.id_source = td.id_source 
            WHERE s.id_source = '{source}' AND td.mnemonique = '{datatype}';
        """)
    data = cur.fetchall()
    response_data = json.loads(json.dumps(data[0][0])) if data else []
    resultat = {
        "code": 200,
        "message": "success",
        "data": response_data
    }
    return json.dumps(resultat)

@app.route('/data')
def get_data():
    """
    Route pour récupérer les données
    """
    datatype = request.args.get('datatype')
    date_fin = request.args.get('date_fin')
    date_debut = request.args.get('date_debut')
    station = request.args.get('station')
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute(f"""SELECT station.id_station, typedata.nom_table
        FROM station
        INNER JOIN typedata ON station.id_source = typedata.id_source
        WHERE typedata.mnemonique = '{datatype}';
    """)
    data = cur.fetchall()
    data_result = list(set([d[0] for d in data[:-1]]))
    table = data[-1][1]
    if station in data_result:
        cur.execute(f"""SELECT json_agg({table}.*)
            FROM {table}
            WHERE {table}.poste = '{station}' AND date BETWEEN '{date_debut}' AND '{date_fin}';
        """)
        data = cur.fetchall()
        response_data = json.loads(json.dumps(data[0][0])) if data else []
    else:
        response_data = []
    resultat = {
        "code": 200,
        "message": "success",
        "data": response_data
    }
    return json.dumps(resultat)

@app.route('/sources')
def get_sources():
    """
    Route pour récupérer les sources    
    """
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute("SELECT json_agg(source.*) FROM source")
    data = cur.fetchall()
    response_data = json.loads(json.dumps(data[0][0])) if data else []
    resultat = {
        "code": 200,
        "message": "success",
        "data": response_data
    }
    return json.dumps(resultat)

if __name__=="__main__":
    main()
