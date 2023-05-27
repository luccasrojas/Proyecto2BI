"""
ETL desarrollado por metateoremas
Autores: 
Brian Manuel Rivera
Tony Santiago Montes
Luccas Rojas
"""

import pandas as pd
import numpy as np
import psycopg2



file_name = "data/Datos_proyecto_II_BI_2017.csv"
data=pd.read_csv(file_name, sep=',', encoding = 'ISO-8859-1', index_col=0)

data =data[["NVCBP8A","NVCBP8G","NVCBP11AA","NVCBP11D","NVCBP14B","NVCBP14I","NHCCPCTRL2","NHCCP20","NHCCP23","NHCCP27","NHCCP31","NHCCP32","NHCCP36A","NHCCP40B","NHCCP40N","NPCFP14I","NVCBP14A"]]
data =data.rename(columns={"NPCFP14I":"ASMA"})


