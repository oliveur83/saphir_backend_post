import psycopg2
def creation_bdd():
    # Connexion à la base de données PostgreSQL
    """
    connexion a la base de donnée, création des 6 tables 
    insertion de toute les donnée sources et station  
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
    # Création d'un curseur
    cur = conn.cursor()
    # Requête SQL pour créer une table
    create_table_wind_barani = '''
    CREATE TABLE IF NOT EXISTS  wind_barani (
        date timestamp PRIMARY KEY,
        timestanp VARCHAR(255),
        station VARCHAR(255) NOT NULL,
        battery VARCHAR(255) NOT NULL,
        wdir_avg VARCHAR(255) NOT NULL,
        wdir_gust VARCHAR(255) NOT NULL,
        wdir_max VARCHAR(255) NOT NULL,
        wdir_min VARCHAR(255) NOT NULL,
        wind_avg VARCHAR(255) NOT NULL,
        wind_max VARCHAR(255) NOT NULL,
        wind_min VARCHAR(255) NOT NULL,
        wind_stdev VARCHAR(255) NOT NULL,
        wdir_stdev VARCHAR(255) NOT NULL,
        id_source VARCHAR(20)
    );
    '''
    create_table2_rain_barani = '''
    CREATE TABLE IF NOT EXISTS  rain_barani (
        date TIMESTAMP primary key ,
        timestanp VARCHAR(255),
        station VARCHAR NOT NULL,
        battery VARCHAR NOT NULL,
        rain VARCHAR,
        rainfall_rate_max VARCHAR,
        dew_point VARCHAR,
        humidity VARCHAR,
        irradiation VARCHAR,
        pressure VARCHAR,
        temperature VARCHAR,
        temperature_wetbulb_stull2011_C VARCHAR,
        id_source VARCHAR(20)
    );
        ''' 
    create_table3_meteo_france_wind2 = '''
    CREATE TABLE IF NOT EXISTS meteo_france_Wind2m_Rian_temp (
        POSTE VARCHAR(20),
        DATE TIMESTAMP,
        RR1 VARCHAR(20),
        DRR1 VARCHAR(20),
        T VARCHAR(20),
        FF2 VARCHAR(20),
        DD2 VARCHAR(20), 
        id_source VARCHAR(20)
    );
        '''
    create_table4_meteo_france_wind10 = '''
    CREATE TABLE IF NOT EXISTS meteo_france_Wind10m_Inso_RelaMoist_StatPress (
        POSTE VARCHAR(20),
        DATE TIMESTAMP,
        PSTAT VARCHAR(20),
        FF VARCHAR(20),
        DD VARCHAR(20),
        U VARCHAR(20),
        INS VARCHAR(20), 
        id_source VARCHAR(20)
    );
        '''
    create_table_data_type_query = '''
    CREATE TABLE IF NOT EXISTS typedata (

        mnemonique VARCHAR(200),
        libelle VARCHAR(200),
        unité VARCHAR(200),
        pas_de_temps VARCHAR(200),
        id_source VARCHAR(200) REFERENCES source(id_source),
        nom_table VARCHAR(200)

    );
        '''
    create_table5_source_station = '''
CREATE TABLE IF NOT EXISTS source (
    id_source VARCHAR(20) PRIMARY KEY,
    nom VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS station (
    id_station VARCHAR(20) PRIMARY KEY,
    nom VARCHAR(100),
    longitude VARCHAR(100),
    latitude VARCHAR(100),
    altitude VARCHAR(100),
    id_source VARCHAR(20) REFERENCES source(id_source)
);'''
    remplissage_source='''
INSERT INTO public.source(
	id_source,nom)
	VALUES (0,'barani')ON CONFLICT DO NOTHING;
 INSERT INTO public.source(
	id_source,nom)
	VALUES (1,'meteo france')ON CONFLICT DO NOTHING;'''
    remplissage_station='''

INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20004002, 'ajaccio', 1,'8°47"33"E','41°55"04"N',5)ON CONFLICT DO NOTHING;
INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20004003, 'ajaccio_parata', 1,'8°37"05"E','41°54"29"N',124)ON CONFLICT DO NOTHING;
INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20007400, 'vergio', 1,'8°53"30"E','42°17"12"N',1420)ON CONFLICT DO NOTHING;

INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20031400, 'bastelica', 1,'9°07"23"E','41°59"57"N',1623)ON CONFLICT DO NOTHING;
INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20040004, 'bocognano', 1,'9°06"11"E','42°06"15"N',1019)ON CONFLICT DO NOTHING;
INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20041001, 'cap pertusato', 1,'9°10"41"E','41°22"29"N',107)ON CONFLICT DO NOTHING;

INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20050001, 'calvi', 1,'8°47"29"E','42°31"46"N',57)ON CONFLICT DO NOTHING;
INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20092001, 'conca', 1,'9°20"54"E','41°43"01"N',253)ON CONFLICT DO NOTHING;

INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20093002, 'ile rousse',1,'8°55"21"E','42°37"59"N',142)ON CONFLICT DO NOTHING;
        
INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20096008, 'corte', 1,'9°11"35"E','42°17"18"N',350)ON CONFLICT DO NOTHING;
INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20107001, 'cap corse', 1,'9°21"34"E','43°00"13"N',72)ON CONFLICT DO NOTHING;
	INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20114002, 'figari', 1,'9°06"13"E','41°30"18"N',20)ON CONFLICT DO NOTHING;

INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20148001, 'bastia', 1,'9°29"06"E','42°32"26"N',10)ON CONFLICT DO NOTHING;
    INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20154001, 'MARIGNANA', 1,'8°39"19"E','42°11"19"N',515)ON CONFLICT DO NOTHING;
INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20185003, 'oletta', 1,'9°19"13"E','42°37"56"N',75)ON CONFLICT DO NOTHING;

INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20223001, 'pietralba', 1,'9°10"19"E','42°32"27"N',510)ON CONFLICT DO NOTHING;
INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20232002, 'pila-canale',1,'8°54"13"E','41°48"52"N',407)ON CONFLICT DO NOTHING;
	INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20247001, 'la chiappa', 1,'9°21"47"E','41°35"41"N',57)ON CONFLICT DO NOTHING;

INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20254006, 'QUENZA', 1,'9°08"34"E','41°46"39"N',932)ON CONFLICT DO NOTHING;

INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20258001, 'renno', 1,'8°48"25"E','42°11"24"N',755)ON CONFLICT DO NOTHING;
INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20268001, 'sampolo', 1,'9°07"22"E','41°56"34"N',837)ON CONFLICT DO NOTHING;
INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20270001, 'sari d orcino', 1,'8°48"08"E','42°04"42"N',407)ON CONFLICT DO NOTHING;

INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20272004, 'sartene', 1,'8°58"45"E','41°39"08"N',62)ON CONFLICT DO NOTHING;
INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20281001, 'cap sagro', 1,'9°29"19"E','42°47"53"N',111)ON CONFLICT DO NOTHING;
INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20303001, 'san guiliano inra', 1,'9°31"18"E','42°17"12"N',47)ON CONFLICT DO NOTHING;

INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20303002, 'alistro	', 1,'9°32"29"E','42°15"34"N',65)ON CONFLICT DO NOTHING;
INSERT INTO public.station(
	id_station, nom, id_source,longitude,latitude,altitude)
	VALUES (20342001, 'solenzara', 1,'9°24"02"E','41°55"18"N',12)ON CONFLICT DO NOTHING;
'''
    remplissage_data_type='''
    INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('RR1','HAUTEUR DE PRECIPITATIONS HORAIRE','MILLIMETRES ET 1/10','horaire',1,'meteo_france_Wind2m_Rian_temp')ON CONFLICT DO NOTHING;
    INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('DRR1','DUREE DES PRECIPITATIONS HORAIR','MINUTES','horaire',1,'meteo_france_Wind2m_Rian_temp')ON CONFLICT DO NOTHING;
     INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('T','	TEMPERATURE SOUS ABRI HORAIRE','DEG C ET 1/10','horaire',1,'meteo_france_Wind2m_Rian_temp')ON CONFLICT DO NOTHING;
     INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('FF2','VITESSE DU VENT A 2 METRES HORAIRE','	M/S ET 1/10','horaire',1,'meteo_france_Wind2m_Rian_temp')ON CONFLICT DO NOTHING;   
     INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('DD2','DIRECTION DU VENT A 2 METRES HORAIRE','ROSE DE 360','horaire',1,'meteo_france_Wind2m_Rian_temp')ON CONFLICT DO NOTHING;
    
    
    INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('PSTAT','PRESSION STATION HORAIRE','HPA ET 1/10','horaire',1,'meteo_france_Wind10m_Inso_RelaMoist_StatPress')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('FF','VITESSE DU VENT HORAIRE','	M/S ET 1/10','horaire',1,'meteo_france_Wind10m_Inso_RelaMoist_StatPress')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('FF','DIRECTION DU VENT A 10 M HORAIRE','	ROSE DE 360','horaire',1,'meteo_france_Wind10m_Inso_RelaMoist_StatPress')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('U','	HUMIDITE RELATIVE HORAIR','%','horaire',1,'meteo_france_Wind10m_Inso_RelaMoist_StatPress')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('INS','	DUREE D_INSOLATION HORAIRE','MINUTES','horaire',1,'meteo_france_Wind10m_Inso_RelaMoist_StatPress')ON CONFLICT DO NOTHING;
   
    
    INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Battery','Battery','','horaire',0,'rain_barani')ON CONFLICT DO NOTHING;
    INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Rain','Rain','','horaire',0,'rain_barani')ON CONFLICT DO NOTHING;
   
        INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Rainfall_rate_max','Rainfall_rate_max','','horaire',0,'rain_barani')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('DewPoint','DewPoint','','horaire',0,'rain_barani')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Humidity','Humidity','','horaire',0,'rain_barani')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Irradiation','Irradiation','','horaire',0,'rain_barani')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Pressure','Pressure','','horaire',0,'rain_barani')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Temperature','Temperature','','horaire',0,'rain_barani')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Temperature_wetbulb_stull2011_C','Temperature_wetbulb_stull2011_C','','horaire',0,'rain_barani')ON CONFLICT DO NOTHING;
   
    
    INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Battery','Battery','','horaire',0,'wind_barani')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Wdir_Avg10','Wdir_Avg10','','horaire',0,'wind_barani')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Wdir_Gust10','Wdir_Gust10','','horaire',0,'wind_barani')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Wdir_Max10','Wdir_Max10','','horaire',0,'wind_barani')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Wdir_Min10','Wdir_Min10','','horaire',0,'wind_barani')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Wind_Avg10','Wind_Avg10','','horaire',0,'wind_barani')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Wind_Max10','Wind_Max10','','horaire',0,'wind_barani')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Wind_Min10','Wind_Min10','','horaire',0,'wind_barani')ON CONFLICT DO NOTHING;
       INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Wind_Stdev10','Wind_Stdev10','','horaire',0,'wind_barani')ON CONFLICT DO NOTHING;
          INSERT INTO public.typedata(mnemonique,libelle,unité,pas_de_temps,id_source,nom_table
	)
	VALUES ('Wdir_Stdev10','Wdir_Stdev10','','horaire',0,'wind_barani')ON CONFLICT DO NOTHING;

     '''
    # Exécution de la requête SQL
    cur.execute(create_table2_rain_barani)
    cur.execute(create_table3_meteo_france_wind2)
    cur.execute(create_table4_meteo_france_wind10)
    cur.execute(create_table_wind_barani)
    cur.execute(create_table5_source_station)
    cur.execute(create_table_data_type_query)
    cur.execute(remplissage_source)
    cur.execute(remplissage_station)
    cur.execute(remplissage_data_type)
    conn.commit()
    conn.close()
