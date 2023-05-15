import csv
import pandas as pd
import psycopg2
from datetime import datetime
#variable global pour les fichier (inconforme)
level_up=False

level2_up=False
cpt=0

def barani_1_cat(chemin_dossier_baranie,nom_fichier,conn):
    """
    Lit un fichier de données de vent et insère les données dans une table de base de données PostgreSQL.

    Args:
        chemin_dossier_baranie (str): Le chemin d'accès au dossier contenant les fichiers de données de vent.
        nom_fichier (str): Le nom du fichier à traiter.
        conn (psycopg2.extensions.connection): La connexion de base de données PostgreSQL à utiliser pour insérer les données.

    Returns:
        None

    Raises:
        None
    """
    global level_up,cpt
    # fais le chemin complet pour lecture 
    chemin_fichier=chemin_dossier_baranie+'/'+nom_fichier 
     # Détermine si le fichier est le premier fichier ou le deuxième fichier pour 13 champs
    if nom_fichier =="2108SW031-2022-04-12.csv":
        level_up=False
    if nom_fichier =="2108SW030-2022-04-21.csv" or nom_fichier =="2108SW031-2022-04-21.csv" :
       
        debut = pd.read_csv(chemin_fichier, delimiter=";", nrows=85)
        
        dfin = pd.read_csv(chemin_fichier, delimiter=";",skiprows=87, header=None,usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12])
        level_up=True
    else:
       
        if level_up==False:
            df = pd.read_csv(chemin_fichier, delimiter=";")
        else:
            print("ot")
            df = pd.read_csv(chemin_fichier, delimiter=";", header=None,skiprows=1,usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12])
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
    # Obtenir les valeurs de chaque colonne pour les 13 champs 

    if nom_fichier =="2108SW030-2022-04-21.csv" or nom_fichier =="2108SW031-2022-04-21.csv":
        
        debut['date'] = debut['date'].astype(str)
        date= debut['date'].tolist()
       
        date = [datetime.strptime(date_str, '%d.%m.%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S') for date_str in date]
        
        stations = debut['station'].tolist()
        batteries = debut['Battery'].tolist()
        wdir_avgs = debut['Wdir_Avg10'].tolist()
        wdir_gusts = debut['Wdir_Gust10'].tolist()
        wdir_maxs = debut['Wdir_Max10'].tolist()
        wdir_mins = debut['Wdir_Min10'].tolist()
        wind_avgs = debut['Wind_Avg10'].tolist()
        wind_maxs = debut['Wind_Max10'].tolist()
        wind_mins = debut['Wind_Min10'].tolist()
        wind_stdevs = debut['Wind_Stdev10'].tolist()
        wdir_stdevs = debut['Wdir_Stdev10'].tolist()
        i=0
        while i< (len(date)):
            # TODO : if nom fichier 2108SW030-2022-04-21.csv faire 86 et apres 
            #print(date[i], stations[i], batteries[i], wdir_avgs[i], wdir_gusts[i], wdir_maxs[i], wdir_mins[i], wind_avgs[i], wind_maxs[i], wind_mins[i], wind_stdevs[i], wdir_stdevs[i])
            cur.execute("INSERT INTO wind_barani (date, station, battery, wdir_avg, wdir_gust, wdir_max, wdir_min, wind_avg, wind_max, wind_min, wind_stdev, wdir_stdev) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)ON CONFLICT DO NOTHING", (date[i], stations[i], batteries[i], wdir_avgs[i], wdir_gusts[i], wdir_maxs[i], wdir_mins[i], wind_avgs[i], wind_maxs[i], wind_mins[i], wind_stdevs[i], wdir_stdevs[i]))
            cpt=cpt+1
            print(" nombre total :",cpt ,"fichier ", i,"nom fichier",nom_fichier)
            i=i+1
            conn.commit()
        
        dfin[0] = dfin[0].astype(str)
        date2= dfin[0].tolist()
       
        date2 = [datetime.strptime(date_str, '%d.%m.%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S') for date_str in date2]
        timetime= dfin[1].tolist()
        stations = dfin[2].tolist()
        batteries = dfin[3].tolist()
        wdir_avgs = dfin[4].tolist()
        wdir_gusts = dfin[5].tolist()
        wdir_maxs = dfin[6].tolist()
        wdir_mins = dfin[7].tolist()
        wind_avgs = dfin[8].tolist()
        wind_maxs = dfin[9].tolist()
        wind_mins = dfin[10].tolist()
        wind_stdevs = dfin[11].tolist()
        
        wdir_stdevs = dfin[12].tolist()
        i=0
        while i< (len(date2)):
            # TODO : if nom fichier 2108SW030-2022-04-21.csv faire 86 et apres 
            #print(date[i], stations[i], batteries[i], wdir_avgs[i], wdir_gusts[i], wdir_maxs[i], wdir_mins[i], wind_avgs[i], wind_maxs[i], wind_mins[i], wind_stdevs[i], wdir_stdevs[i])
            cur.execute("INSERT INTO wind_barani (date, timestanp, station, battery, wdir_avg, wdir_gust, wdir_max, wdir_min, wind_avg, wind_max, wind_min, wind_stdev, wdir_stdev) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)ON CONFLICT DO NOTHING", (date2[i],timetime[i], stations[i], batteries[i], wdir_avgs[i], wdir_gusts[i], wdir_maxs[i], wdir_mins[i], wind_avgs[i], wind_maxs[i], wind_mins[i], wind_stdevs[i], wdir_stdevs[i]))
            cpt=cpt+1
            print(" nombre total :",cpt ,"fichier ", i,"nom fichier",nom_fichier)
            i=i+1
            conn.commit()

        # Fermer le curseur et la connexion à la base de données
        cur.close()
        conn.close()
        
    else:
        # pour les 12 champs et les 13 champs sur fichier complet 
        if level_up==False:
            date= df['date'].tolist()
           
            date = [datetime.strptime(date_str, '%d.%m.%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S') for date_str in date]
            timetime= [''] * len(date)
            stations = df['station'].tolist()
            batteries = df['Battery'].tolist()
            wdir_avgs = df['Wdir_Avg10'].tolist()
            wdir_gusts = df['Wdir_Gust10'].tolist()
            wdir_maxs = df['Wdir_Max10'].tolist()
            wdir_mins = df['Wdir_Min10'].tolist()
            wind_avgs = df['Wind_Avg10'].tolist()
            wind_maxs = df['Wind_Max10'].tolist()
            wind_mins = df['Wind_Min10'].tolist()
            wind_stdevs = df['Wind_Stdev10'].tolist()
            wdir_stdevs = df['Wdir_Stdev10'].tolist()
        else:
            df[0] = df[0].astype(str)
            date= df[0].tolist()
            date.pop(0)

            date = [datetime.strptime(date_str, '%d.%m.%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S') for date_str in date]
            
            stations = df[2].tolist()
            timetime=df[1].tolist()
            batteries = df[3].tolist()
            
            wdir_avgs = df[4].tolist()
            wdir_gusts = df[5].tolist()
       
            wdir_maxs = df[6].tolist()
            wdir_mins = df[7].tolist()
            wind_avgs = df[8].tolist()
            wind_maxs = df[9].tolist()
            wind_mins = df[10].tolist()
            wind_stdevs = df[11].tolist()
            
            wdir_stdevs = df[12].tolist()
        # insertion dans la base de donnée 
        i=0
        while i< (len(date)):
            cur.execute("INSERT INTO wind_barani (date,timestanp, station, battery, wdir_avg, wdir_gust, wdir_max, wdir_min, wind_avg, wind_max, wind_min, wind_stdev, wdir_stdev,id_source) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)ON CONFLICT DO NOTHING", (date[i],timetime[i], stations[i], batteries[i], wdir_avgs[i], wdir_gusts[i], wdir_maxs[i], wdir_mins[i], wind_avgs[i], wind_maxs[i], wind_mins[i], wind_stdevs[i], wdir_stdevs[i],'0'))
            cpt=cpt+1
            print(" nombre total :",cpt ,"fichier ", i,"nom fichier",nom_fichier)
            i=i+1
            conn.commit()

        # Fermer le curseur et la connexion à la base de données
        cur.close()
        conn.close()

def barani_2_cat(chemin_dossier_baranie,nom_fichier,conn):
    """
    Cette fonction prend en entrée le chemin du dossier contenant les fichiers Barani, le nom du fichier Barani à traiter et une connexion à une base de données PostgreSQL.
    Elle insère les données contenues dans le fichier Barani dans la table "rain_barani" de la base de données PostgreSQL.
    Args:
        chemin_dossier_baranie (str): Chemin vers le dossier contenant le fichier à lire.
        nom_fichier (str): Nom du fichier contenant les données météorologiques.
        conn (psycopg2.extensions.connection): La connexion de base de données PostgreSQL à utiliser pour insérer les données.

    Returns:
        None
    """
    global cpt,level2_up
    # Chemin du fichier Barani
    chemin_fichier=chemin_dossier_baranie+'/'+nom_fichier    
    if nom_fichier=='2008SH025-2022-04-14.csv':
        df = pd.read_csv(chemin_fichier, delimiter=";", header=None,skiprows=2,usecols=[0,1,2,3,4,5,6,7,8,9,10])
        level2_up=True
    else:    
        if level2_up==True:
            df = pd.read_csv(chemin_fichier, delimiter=";", header=None,skiprows=2,usecols=[0,1,2,3,4,5,6,7,8,9,10])
        else:
            # Lecture du fichier Barani
            df = pd.read_csv(chemin_fichier, delimiter=";", header=None,skiprows=2,usecols=[0,1,2,3,4,5,6,7,8,9,10,11])
    # Informations de connexion à la base de données PostgreSQL
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
    #mettre la date au bont format 
    df[0] = df[0].astype(str)
    # Extraction des dates et conversion en format "AAAA-MM-JJ HH:MM:SS"
    date= df[0].tolist()
    date.pop(0)
    date = [datetime.strptime(date_str, '%d.%m.%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S') for date_str in date]
    if level2_up==False:
        # Extraction des autres colonnes
        station = df[2].tolist()
        #timetime est timestamp
        timetime=df[1].tolist()
        battery = df[3].tolist()
        rain = df[4].tolist()
        rainfall_rate_max = df[5].tolist()
        dewpoint = df[6].tolist()
        humidity = df[7].tolist()
        irradation = df[8].tolist()
        pressure= df[9].tolist()
        Temperature = df[10].tolist()

        Temperature_wetbulb_stull2011_C = df[11].tolist()
        #insertion dans la base de donnée 
        i=0 
        while i <len(date):

            cur.execute("INSERT INTO rain_barani (date,timestanp, station, battery, rain, rainfall_rate_max, dew_point, humidity, irradiation, pressure, temperature, temperature_wetbulb_stull2011_C,id_source) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s) ON CONFLICT DO NOTHING", (date[i],timetime[i], station[i], battery[i], rain[i], rainfall_rate_max[i], dewpoint[i], humidity[i], irradation[i], pressure[i], Temperature[i], Temperature_wetbulb_stull2011_C[i],'0'))
            cpt=cpt+1
            print(" nombre total :",cpt ,"fichier ", i,"nom fichier",nom_fichier)
            i=i+1
            conn.commit()
        cur.close()
        conn.close()
    else:
        # Extraction des autres colonnes
        station = df[1].tolist()
        #timetime est timestamp
        timetime=['']*len(station)
        battery = df[2].tolist()
        rain = df[3].tolist()
        rainfall_rate_max = df[4].tolist()
        dewpoint = df[5].tolist()
        humidity = df[6].tolist()
        irradation = df[7].tolist()
        pressure= df[8].tolist()
        Temperature = df[9].tolist()

        Temperature_wetbulb_stull2011_C = df[10].tolist()
        #insertion dans la base de donnée 
        i=0 
        while i <len(date):

            cur.execute("INSERT INTO rain_barani (date,timestanp, station, battery, rain, rainfall_rate_max, dew_point, humidity, irradiation, pressure, temperature, temperature_wetbulb_stull2011_C) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", (date[i],timetime[i], station[i], battery[i], rain[i], rainfall_rate_max[i], dewpoint[i], humidity[i], irradation[i], pressure[i], Temperature[i], Temperature_wetbulb_stull2011_C[i]))
            cpt=cpt+1
            print(" nombre total :",cpt ,"fichier ", i,"nom fichier",nom_fichier)
            i=i+1
            conn.commit()
        cur.close()
        conn.close()

# Fonction pour convertir la date de "aaaa-mm-jj" à "jj/mm/aaaa"
def convert_date(date_str_list):
    date_list = []
    for date_str in date_str_list:
        date = datetime.strptime(date_str, '%Y%m%d%H')
        timestamp = int(date.timestamp())
        date_list.append(datetime.fromtimestamp(timestamp).strftime('%d.%m.%Y %H:%M:%S'))
    return date_list

def meteo_fr_1_cat(chemin_dossier, nom_fichier):
    """Insertion des données météorologiques dans une base de données PostgreSQL.

    Args:
        chemin_dossier (str): Chemin vers le dossier contenant le fichier à lire.
        nom_fichier (str): Nom du fichier contenant les données météorologiques.

    Returns:
        None
    Remarques:
    - Le fichier à insérer doit être au format CSV et doit contenir les colonnes suivantes :
        - POSTE : str - identifiant de la station météorologique
        - DATE : str - date et heure de l'observation au format "AAAA-MM-JJ hh:mm:ss"
        - RR1 : Quantité de précipitations tombées en 1 heure en mm
        - DRR1 : Durée de la période de temps pendant laquelle les précipitations sont tombées
        - T : Température en degrés Celsius
        - FF2 : Vitesse moyenne du vent en m/s
        - DD2 : Direction moyenne du vent en degrés
  - Les données sont insérées dans la table "meteo_data" en utilisant une requête SQL paramétrée.
    - Si une observation est déjà présente dans la base de données pour une même station et à une même date,
      alors la nouvelle observation ne sera pas insérée grâce à la clause "ON CONFLICT DO NOTHING".
    - La fonction gère la conversion de la date au format "AAAA-MM-JJ hh:mm:ss" en un objet datetime pour l'insertion dans la base de données.
   
    """
    global cpt
    # Chemin complet du fichier
    chemin_fichier = chemin_dossier + '/' + nom_fichier
    
    # Lecture du fichier CSV avec pandas
    data = pd.read_csv(chemin_fichier, sep=';', header=0, names=["POSTE", "DATE", "RR1", "DRR1", "T", "FF2", "DD2"], dtype={"POSTE":str, "DATE":str, "RR1":str, "DRR1":str, "T":str, "FF2":str, "DD2":str})

    # Accéder aux données de chaque colonne
    POSTE = data["POSTE"].tolist()
    DATE = data["DATE"].tolist()
    DATE = convert_date(DATE) # convertir les dates dans le format souhaité
    RR1 = data["RR1"].tolist()
    DRR1 = data["DRR1"].tolist()
    T = data["T"].tolist()
    FF2 = data["FF2"].tolist()
    DD2 = data["DD2"].tolist()

    # Informations de connexion à la base de données
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
    i=0
    while i<len(POSTE):
         # Insertion des données dans la table "meteo_data"
        cur.execute("INSERT INTO meteo_france_Wind2m_Rian_temp (POSTE, DATE, RR1, DRR1, T, FF2, DD2,id_source) VALUES (%s, %s, %s, %s, %s, %s, %s,%s) ON CONFLICT DO NOTHING", (POSTE[i], DATE[i], RR1[i], DRR1[i], T[i], FF2[i], DD2[i],'1'))
        print(" nombre total :",cpt ,"fichier ", i,"nom fichier",nom_fichier)
        cpt=cpt+1
        i=i+1
        conn.commit()
    # Fermeture de la connexion
    cur.close()
    conn.close()

    
def meteo_fr_2_cat(chemin_fichier,nom_fichier):
    """
    Cette fonction permet d'insérer les données météorologiques françaises de type 2 dans la table "meteo2_data" 
    de la base de données PostgreSQL "saphir".

    Arguments:
    - chemin_fichier : un str représentant le chemin absolu du répertoire où se trouve le fichier à insérer.
    - nom_fichier : un str représentant le nom du fichier à insérer.

    Returns:
    - None

    Remarques:
    - Le fichier à insérer doit être au format CSV et doit contenir les colonnes suivantes :
        - POSTE : str - identifiant de la station météorologique
        - DATE : str - date et heure de l'observation au format "AAAA-MM-JJ hh:mm:ss"
        - PSTAT : str - pression au niveau de la mer en hPa
        - FF : str - vitesse moyenne du vent en km/h
        - DD : str - direction moyenne du vent en degrés
        - U : str - taux d'humidité en %
        - INS : str - quantité de précipitations tombées en mm
    - Les données sont insérées dans la table "meteo2_data" en utilisant une requête SQL paramétrée.
    - Si une observation est déjà présente dans la base de données pour une même station et à une même date,
      alors la nouvelle observation ne sera pas insérée grâce à la clause "ON CONFLICT DO NOTHING".
    - La fonction gère la conversion de la date au format "AAAA-MM-JJ hh:mm:ss" en un objet datetime pour l'insertion dans la base de données.
    """
     # Concaténer le chemin et le nom du fichier
    chemin_fichier=chemin_fichier+'/'+nom_fichier
     # Lire les données CSV dans un dataframe Pandas 
    data = pd.read_csv(chemin_fichier, sep=';', header=0, names=["POSTE","DATE","PSTAT","FF","DD","U","INS"], dtype={"POSTE":str, "DATE":str, "PSTAT":str, "FF":str, "DD":str, "U":str, "INS":str})
    
    # Accéder aux données de chaque colonne
    POSTE = data["POSTE"].tolist()
    DATE = data["DATE"].tolist()
    DATE = convert_date(DATE)
    PSTAT = data["PSTAT"].tolist()
    FF = data["FF"].tolist()
    DD = data["DD"].tolist()
    U = data["U"].tolist()
    INS = data["INS"].tolist()
# Informations de connexion à la base de données
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
    global cpt
    i=0
    while i<len(POSTE):
        
         # Insérer les données dans la table "meteo2_data"
        print(" nombre total :",cpt ,"fichier ", i,"nom fichier",nom_fichier)
        cur.execute("INSERT INTO meteo_france_Wind10m_Inso_RelaMoist_StatPress (POSTE, DATE, PSTAT, FF, DD, U, INS,id_source) VALUES (%s, %s, %s, %s, %s, %s, %s,%s) ON CONFLICT DO NOTHING", (POSTE[i], DATE[i], PSTAT[i], FF[i], DD[i], U[i], INS[i],'1'))
        i=i+1
        cpt=cpt+1
        conn.commit()
    cur.close()
    conn.close()