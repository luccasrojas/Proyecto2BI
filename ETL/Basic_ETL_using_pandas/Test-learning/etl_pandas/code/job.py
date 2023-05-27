import pandas as pd
import uuid
# extracting data from filesystem
# IMported required libraries and modules
import sys
# If you are not able to import constant py file use below code

from extract import extract
from transform import rename_cols, join_df, specific_cols
from load import load

#### ----- Extract ----- ####

# Extracting CITY and COUNTRY data from MYSQL
# city_df = extract("db","city")
# country_df = extract("db","country")

# Extracting COUNTRYLANGUAGE data from FileSystem
Asma_DF = extract("csv","ETL\Basic_ETL_using_pandas\Test-learning\etl_pandas\data\Datos_proyecto_II_BI_2017.csv")
NoAsma_DF = extract("csv","ETL\Basic_ETL_using_pandas\Test-learning\etl_pandas\data\Datos_proyecto_II_BI_2017_sin_asma.csv")

# Get specific cols
Asma_DF = Asma_DF[["NVCBP8A","NVCBP8G","NVCBP11AA","NVCBP11D","NVCBP14B","NVCBP14I","NHCCPCTRL2","NHCCP20","NHCCP23","NHCCP27","NHCCP31","NHCCP32","NHCCP36A","NHCCP40B","NHCCP40N","NPCFP14I","NVCBP14A"]]
NoAsma_DF = NoAsma_DF[["NVCBP8A","NVCBP8G","NVCBP11AA","NVCBP11D","NVCBP14B","NVCBP14I","NHCCPCTRL2","NHCCP20","NHCCP23","NHCCP27","NHCCP31","NHCCP32","NHCCP36A","NHCCP40B","NHCCP40N","NPCFP14I","NVCBP14A"]]

# Join DF with common columns
data = pd.concat([Asma_DF, NoAsma_DF], axis=0, ignore_index=True)
data['PersonUUID'] = data.apply(lambda _: uuid.uuid4(), axis=1) # PersonUUID

# Rename Columns
column_dict = {
"NPCFP14I": "DiagnosticadoAsma",
"NVCBP8A": "PoseeHumedad",
"NVCBP8G": "EscasaVentilacion",
"NVCBP11AA": "Estrato",
"NVCBP11D": "ServicioRecoleccionBasuras",
"NVCBP14B": "CercaBasureros",
"NVCBP14I": "CercaCanos",
"NHCCPCTRL2": "NumPersonasHogar",
"NHCCP20": "NumDormitorios",
"NHCCP23": "LugarPreparacionComida",
"NHCCP27": "FuenteAgua",
"NHCCP31": "TipoServicioSanitario",
"NVCBP14A": "CercaFabricas",
"NHCCP40N": "PoseeAnimales",
"NHCCP40B": "PoseeNevera",
"NHCCP36A": "PoseeLavamanos",
"NHCCP32": "NumSanitarios"
}
data = rename_cols(data, column_dict)

# Creacion de las dimensiones
# Dimension 1: Condicion de la vivienda
columnas_condicion_vivienda = ["PoseeHumedad","EscasaVentilacion","Estrato","ServicioRecoleccionBasuras"]
dimension_condicion_vivienda = specific_cols(data, columnas_condicion_vivienda)
dimension_condicion_vivienda['CondicionesViviendaUUID'] = data.apply(lambda _: uuid.uuid4(), axis=1) # CondicionesViviendaUUID

# Dimension 2: Entorno de la vivienda
columnas_entorno_vivienda = ["CercaBasureros","CercaCanos","CercaFabricas"]
dimension_entorno_vivienda = specific_cols(data, columnas_entorno_vivienda)
dimension_entorno_vivienda['EntornoViviendaUUID'] = data.apply(lambda _: uuid.uuid4(), axis=1) # EntornoViviendaUUID

# Dimension 3: Manjeo de la comida
columnas_manejo_comida = ["LugarPreparacionComida","FuenteAgua","PoseeLavamanos","PoseeNevera"]
dimension_manejo_comida = specific_cols(data, columnas_manejo_comida)
dimension_manejo_comida['ManejoComidaUUID'] = data.apply(lambda _: uuid.uuid4(), axis=1) # ManejoComidaUUID

# Dimension 4: Condiciones habitacionales
columnas_condiciones_habitacionales = ["NumPersonasHogar","NumDormitorios","NumSanitarios","TipoServicioSanitario","PoseeAnimales"]
dimension_condiciones_habitacionales = specific_cols(data, columnas_condiciones_habitacionales)
dimension_condiciones_habitacionales['CondicionesHabitacionalesUUID'] = data.apply(lambda _: uuid.uuid4(), axis=1) # CondicionesHabitacionalesUUID


# Generar la tabla de hechos
tabla_hechos = pd.DataFrame()

# Unir las dimensiones con la tabla de hechos
tabla_hechos['PersonUUID'] = data['PersonUUID']
tabla_hechos['CondicionesViviendaUUID'] = dimension_condicion_vivienda['CondicionesViviendaUUID']
tabla_hechos['EntornoViviendaUUID'] = dimension_entorno_vivienda['EntornoViviendaUUID']
tabla_hechos['ManejoComidaUUID'] = dimension_manejo_comida['ManejoComidaUUID']
tabla_hechos['CondicionesHabitacionalesUUID'] = dimension_condiciones_habitacionales['CondicionesHabitacionalesUUID']

# Agregar la columna de Asma
tabla_hechos['DiagnosticadoAsma'] = data['DiagnosticadoAsma']

# Crear las columnas calculadas
# Para personas por dormitorio y personas por inodoro aproximado a 2 decimales
dimension_condiciones_habitacionales["PersonasPorDormitorio"] = round(dimension_condiciones_habitacionales["NumPersonasHogar"]/dimension_condiciones_habitacionales["NumDormitorios"],2)
dimension_condiciones_habitacionales["PersonasPorInodoro"] = round(dimension_condiciones_habitacionales["NumPersonasHogar"]/dimension_condiciones_habitacionales["NumSanitarios"],2)

# Eliminar las columnas de numero de personas por hogar, numero de dormitorios y numero de sanitarios
dimension_condiciones_habitacionales = dimension_condiciones_habitacionales.drop(["NumPersonasHogar","NumDormitorios","NumSanitarios"], axis=1)

# MySQL 
constraint_primary_key = 'ALTER TABLE public."{0}" ADD CONSTRAINT "{1}" PRIMARY KEY ("{2}");'
constraint_foreing_key = 'ALTER TABLE public."{0}" ADD CONSTRAINT "{1}" FOREIGN KEY ("{2}") REFERENCES public."{3}"("{4}");'
constraint_unique = 'ALTER TABLE public."{0}" ADD CONSTRAINT "{1}" UNIQUE ("{2}");'

# Donde {0} es el nombre de la tabla, {1} es el nombre de la llave primaria, {2} es el nombre de la columna de la llave primaria
# Donde {3} es el nombre de la tabla a la que se hace referencia, {4} es el nombre de la columna de la llave foranea 

load("db",dimension_condiciones_habitacionales, "CondicionesHabitacionales", [
    constraint_unique.format('CondicionesHabitacionales', 'CondicionesHabitacionalesU', 'CondicionesHabitacionalesUUID'),
    constraint_primary_key.format('CondicionesHabitacionales', 'CondicionesHabitacionalesPK', 'CondicionesHabitacionalesUUID'),
])
load("db",dimension_manejo_comida, "ManejoComida", [
    constraint_unique.format('ManejoComida', 'ManejoComidaU', 'ManejoComidaUUID'),
    constraint_primary_key.format('ManejoComida', 'ManejoComidaPK', 'ManejoComidaUUID'),
])
load("db",dimension_entorno_vivienda, "EntornoVivienda", [
    constraint_unique.format('EntornoVivienda', 'EntornoViviendaU', 'EntornoViviendaUUID'),
    constraint_primary_key.format('EntornoVivienda', 'EntornoViviendaPK', 'EntornoViviendaUUID'),
])
load("db",dimension_condicion_vivienda, "CondicionesVivienda", [
    constraint_unique.format('CondicionesVivienda', 'CondicionesViviendaU', 'CondicionesViviendaUUID'),
    constraint_primary_key.format('CondicionesVivienda', 'CondicionesViviendaPK', 'CondicionesViviendaUUID'),
])
load("db",tabla_hechos, "HigieneFacts", [
    constraint_primary_key.format("HigieneFacts", 'HigieneFactsPK', 'PersonUUID'),
    constraint_foreing_key.format("HigieneFacts", 'HigieneFactsFK1', 'CondicionesViviendaUUID', 'CondicionesVivienda', 'CondicionesViviendaUUID'),
    constraint_foreing_key.format("HigieneFacts", 'HigieneFactsFK2', 'EntornoViviendaUUID', 'EntornoVivienda', 'EntornoViviendaUUID'),
    constraint_foreing_key.format("HigieneFacts", 'HigieneFactsFK3', 'ManejoComidaUUID', 'ManejoComida', 'ManejoComidaUUID'),
    constraint_foreing_key.format("HigieneFacts", 'HigieneFactsFK4', 'CondicionesHabitacionalesUUID', 'CondicionesHabitacionales', 'CondicionesHabitacionalesUUID'),
])






# #### ----- Transformation ----- ####

# # 1. Rename Columns
# city_df = rename_cols(city_df, CITY_COL_DICT)
# country_df = rename_cols(country_df, COUNTRY_COL_DICT)
# country_language_df = rename_cols(country_language_df, COUNTRY_LANGUAGE_COL_DICT)

# # 2. Join DF with common column "country_code"
# country_city_df=join_df(country_df, city_df, JOIN_ON_COLUMNS, JOIN_TYPE)
# country_city_language_df= join_df(country_city_df, country_language_df, JOIN_ON_COLUMNS, JOIN_TYPE)

# # 3. Get specific cols
# country_city_language_df = specific_cols(country_city_language_df, SPEC_COLS)



# #### ----- Load Data ----- ####

# # MySQL 
# load("db",country_city_language_df, "countrycitylanguage")

# # FileSystem
# load("csv",country_city_language_df, "etl_pandas/output/countrycitylanguage.csv")



    
     


    
    
