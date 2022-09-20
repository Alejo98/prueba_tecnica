# importar librerías necesarias para ejecutar el código
import requests
import json
import pandas as pd
from pandas.io.json import json_normalize

import datetime

# perfilamiento de datos
import leila
from leila import reporte
from pandas_profiling import ProfileReport


# definición de funciones
def requerimiento_principal(enlace, fecha_inicial, fecha_final):
  '''
  esta consulta se encarga de hacer el requerimiento al API con ayuda
  de la librería request y contiene inmersa la lógica para extraer varios
  días de información
  input:
    enlace: enlace al cual se hará el requerimiento
    fecha_inicial: fecha desde cuando se va a realizar el requerimiento
    fecha_final: fecha hasta cuando se va a realizar el requerimiento

  output: 
    df_main: dataframe con la data acumulada del requerimiento realizado
  '''
  lista_params = rango_tiempo(fecha_inicial, fecha_final)
  
  df_main = requerimiento_data(enlace, lista_params[0])

  for param in lista_params[1:]:
    df = requerimiento_data(enlace, param)
    df_main = pd.concat(([df_main, df]))

  return(df_main)

def rango_tiempo(fecha_inicial, fecha_final):
  '''
  Genera una lista de parámetros de fecha en forma de diccionario para
  hacer un requerimiento desde una fecha inicial a una fecha final.

  input:
    fecha_inicial: fecha desde cuando se va a realizar el requerimiento
    fecha_final: fecha hasta cuando se va a realizar el requerimiento
    
  output:
    list_params: Retorna una lista con elementos en forma de date para
    realizar el requerimiento de la información. 
  '''
  format = "%Y-%m-%d"
  
  fecha_inicial = datetime.datetime.strptime(fecha_inicial, format).date()
  fecha_final = datetime.datetime.strptime(fecha_final, format).date()
  
  list_params = []

  while fecha_inicial <= fecha_final:
    list_params.append({'date':fecha_inicial})
    fecha_inicial += datetime.timedelta(days=1)

  return(list_params)

def requerimiento_data(enlace, params):
   '''
   Realiza un requerimiento a un enlace y a los parametros que se le pasen
   a dicho enlace con la ayuda de la librería request y el método get 
  
   input:
     enlace: enlace al cual se hará el requerimiento
     params: diccionario de parámetros para realizar el requerimiento 
  
   output: 
    return: en caso de que el requerimiento sea 200 (exitoso) envía un
    dataframe con la información solicitada y en caso de que no, envía un
    mensaje de error con los params que generaron el error.
   '''
   r  = requests.get(enlace, params = params)
   if r.status_code == 200:
      data = r.json()
      
      # Writing to sample.json
      with open("json/request_"+params["date"].strftime("%Y-%m-%d")+".json", "w") as outfile:
        json.dump(data, outfile)

      df = pd.DataFrame.from_records(data)
      return(df)
   else:
      print("\n ERROR" + params)

def normalizar_diccionarios(dataframe_pd, column_to_normalize, column_to_id
                            , normalization_type = 'one_to_one'):
  if column_to_normalize in dataframe_pd.columns:
    if normalization_type == 'one_to_one':
      df_embedded = pd.json_normalize(dataframe_pd[column_to_normalize])
      df_embedded['id'] = dataframe_pd[column_to_id]

      list_of_columns = df_embedded.columns
      list_of_columns.drop([column_to_id])
      
      dataframe_pd = dataframe_pd.merge(df_embedded[list_of_columns], 
                                        left_on = 'id', 
                                        right_on = 'id', 
                                        validate = "one_to_one")
      dataframe_pd = dataframe_pd.drop([column_to_normalize], axis=1)
  else:
    print('''la columna que desea normalizar no existe, por lo tanto no se
    ejecuto ninguna normalización''')
  return(dataframe_pd)

