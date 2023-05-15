# 4 table 2 par 2
import psycopg2
from flask import Flask, request
import os 
from lecture_fichier import barani_1_cat,barani_2_cat,meteo_fr_1_cat,meteo_fr_2_cat
from BDD import creation_bdd

import json 

def main():
    """
    connexion a la base de donnée 
    """
    host = "localhost"
    database = "saphir" 
    user = "postgres"
    password = "toto"

    # Connexion à la base de données
    conn = psycopg2.connect(
    host=host,
    database=database,
    user=user,
    password=password
    )
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
    print("baranie reussi 215")
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
    print("meteo reussi")
    """
    recuperation  de ma base de donnée 
    """ 

    host = "localhost"
    database = "saphir" 
    user = "postgres"
    password = "toto"

    # Connexion à la base de données
    conn = psycopg2.connect(
    host=host,
    database=database,
    user=user,
    password=password
    )
    cur = conn.cursor()
    cur.execute(f"SELECT json_agg(source.*) FROM source")
    response_data = []
    data = cur.fetchall()
    data_json = json.loads(json.dumps(data))
    print(data_json)
    if len(data_json) > 0:
        response_data = data_json[0][0]
    resultat={
       "code": 200,
    "message": "success",
    "data": response_data
    }
    return json.dumps(resultat)

app = Flask(__name__)
# #requete http
@app.route('/datatypes')
def get_datatypes():
    source = request.args.get('source')
    host = "localhost"
    database = "saphir" 
    user = "postgres"
    password = "toto"

        # Connexion à la base de données
    conn = psycopg2.connect(
    host=host,
    database=database,
    user=user,
    password=password
        )
    cur = conn.cursor()
    cur.execute(f"""SELECT  json_agg(td.*) 
        FROM typedata td  
        WHERE td.id_source  = '{source}';
        """)
    response_data = []
    data = cur.fetchall()
    data_json = json.loads(json.dumps(data))
    
    if len(data_json) > 0:
        response_data = data_json[0][0]
    resultat={
       "code": 200,
    "message": "success",
    "data": response_data
    }
    return json.dumps(resultat)

@app.route('/station')
def get_station():
        host = "localhost"
        database = "saphir" 
        user = "postgres"
        password = "toto"

        # Connexion à la base de données
        conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
        )
        cur = conn.cursor()
        source = request.args.get('source')
        datatype = request.args.get('datatype')
        # votre code ici
        if datatype is None:
            cur.execute(f"""SELECT json_agg(s) 
                FROM station s 
                JOIN source src ON src.id_source = s.id_source 
                WHERE s.id_source = '{source}';
                    """)
            response_data = []
            data = cur.fetchall()
            data_json = json.loads(json.dumps(data))
            if len(data_json) > 0:
                response_data = data_json[0][0]
            
        else:
            cur.execute(f"""SELECT  json_agg(distinct s.*) 
                FROM station s 
                 inner JOIN typedata td ON s.id_source= td.id_source 
                WHERE s.id_source= '{source}' AND td.mnemonique = '{datatype}';

                    """)
            response_data = []
            data = cur.fetchall()
            data_json = json.loads(json.dumps(data))
            print(data_json)
            if len(data_json) > 0:
                response_data = data_json[0][0]      # endpoint 2: retourner les données de la station pour la source et le type de données donnés
        resultat={
       "code": 200,
    "message": "success",
    "data": response_data
    }
        return json.dumps(resultat)
@app.route('/data')
def get_data():
        host = "localhost"
        database = "saphir" 
        user = "postgres"
        password = "toto"

        # Connexion à la base de données
        conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
        )
        datatype = request.args.get('datatype')
        cur = conn.cursor()
        cur.execute(f"""SELECT station.id_station,typedata.nom_table
        from station
        inner join typedata on station.id_source =typedata.id_source
        where  typedata.mnemonique = {datatype}
        """)
        data = cur.fetchall()
    
        data_result=[]
        i=0
        while i<len(data)-1:
            data_result=data_result+[data[i][0]]
            i=i+1
        data_result=list(set(data_result))
        table=data[i][1]
        date_fin = request.args.get('date_fin')
        date_debut = request.args.get('date_debut')
        station = request.args.get('station')
        i=0
        while i<len(data_result)-1:
            if data_result[i]==station:
                cur.execute(f"""SELECT json_agg({table}.*)
                    from {table}
                    where  {table}.poste= '{data_result[i]}' and date between '{date_debut}' and '{date_fin}';
                    """)

        response_data = []
        data = cur.fetchall()
        data_json = json.loads(json.dumps(data))
      
        if len(data_json) > 0:
            response_data = data_json[0][0]
        resultat={
       "code": 200,
    "message": "success",
    "data": response_data
    }
        return json.dumps(resultat)
         
@app.route('/sources')
def get_sources():
    host = "localhost"
    database = "saphir" 
    user = "postgres"
    password = "toto"

    # Connexion à la base de données
    conn = psycopg2.connect(
    host=host,
    database=database,
    user=user,
    password=password
    )
    cur = conn.cursor()
    cur.execute(f"SELECT json_agg(source.*) FROM source")
    response_data = []
    data = cur.fetchall()
    data_json = json.loads(json.dumps(data))
    print(data_json)
    if len(data_json) > 0:
        response_data = data_json[0][0]
    resultat={
       "code": 200,
    "message": "success",
    "data": response_data
    }
    return json.dumps(resultat)
if __name__=="__main__":
    main()
