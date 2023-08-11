import pandas as pd
import numpy as np
import re

def Desanidar(dataset, num_columna):
        
        def NombrarColumna(columna):
                        
                        flag = 0
                        for elemento in columna:
                                if type(elemento) == str and flag == 0:
                                        possep = elemento.find(":")
                                        nombre = dataset.columns[num_columna] + ': ' + elemento[:possep]
                                        flag = 1
                        return nombre
        
        def EliminarNombre(dato):
        
                        if type(dato) == str:
                                possep = dato.find(":")
                                dato = dato.replace(dato[:possep + 1], '')

                        return dato

        if dataset[dataset.columns[num_columna]].dtype == object:

                df = dataset[dataset.columns[num_columna]].str.split(pat= ',', n= -1, expand= True)

                reemplazos = ["{", "}", "'", " ", '[', ']']

                for column in df:

                        for elemento in reemplazos:
    
                                df[column] = df[column].str.replace(elemento, "")
        
                        nombre = NombrarColumna(df[column])
                        df[column] = df[column].apply(EliminarNombre)
                        df.rename(columns= {column: nombre}, inplace= True)
        
                return df
        
        else:
                return print("ERROR: No es posible desanidar correctamente, tipo de dato incorrecto en columna ", num_columna)

def obtener(dic_str):
    id = re.findall(r"{'credit_id': '([^']+)', 'department': 'Directing', 'gender': (\d+), 'id': (\d+), 'job': 'Director', 'name': '([^']+)', 'profile_path': '([^']+)'}", dic_str)
  
    
    return id if id else None

def eliminar_columnas_nulas(df, porcentaje):

        porcentaje_nulos = df.isnull().mean()
        eliminar = porcentaje_nulos[porcentaje_nulos > (porcentaje / 100)].index
        df = df.drop(columns=eliminar)
       

        return df

def retorno(df):

        if df['budget'] > 0:
                return round((df['revenue'] / df['budget'])*100, 0)
        else:
                return 0
        
def insertar_espacio(texto):
    return re.sub(r'(?<=.)([A-Z])', r' \1', texto)

#Importar archivos

movies_df = pd.read_csv("C:/Users/eduen/AppData/Local/Temp/movies_dataset.csv", low_memory= False)
credits_df = pd.read_csv("C:/Users/eduen/AppData/Local/Temp/credits.csv", low_memory= False)

#ETL movies_df
columnas_desanidar = [1, 3, 12, 13, 17]

for i, elemento in enumerate(columnas_desanidar):

    col = Desanidar(movies_df, (elemento - i))
    movies_df = movies_df.drop(movies_df.columns[elemento - i], axis=1)
    movies_df = pd.concat([movies_df, col], axis= 1)

movies_df.drop('video', axis=1, inplace=True)
movies_df.drop('imdb_id', axis=1, inplace=True)
movies_df.drop('adult', axis=1, inplace=True)
movies_df.drop('original_title', axis=1, inplace=True)
movies_df.drop('poster_path', axis=1, inplace=True)
movies_df.drop('homepage', axis=1, inplace=True)
movies_df['budget'] = pd.to_numeric(movies_df['budget'], errors= 'coerce')
movies_df['budget'] = movies_df['budget'].fillna(0)
movies_df['revenue'] = movies_df['revenue'].fillna(0)
movies_df['runtime'] = movies_df['runtime'].fillna(0)
movies_df.dropna(subset=['release_date'], inplace=True)
movies_df['release_date'] = pd.to_datetime(movies_df['release_date'], format= '%Y-%m-%d', errors= 'coerce')
movies_df['anio'] = movies_df['release_date'][movies_df['release_date'].notnull()].dt.year
movies_df['return_(%)'] = movies_df.apply(retorno, axis= 1)
movies_df['runtime_(minutos)'] = movies_df['runtime'].round().astype(int)
movies_df.drop('runtime', axis=1, inplace=True)
movies_df['budget_(dolares)'] = movies_df['budget']
movies_df.drop('budget', axis=1, inplace=True)
movies_df['revenue_(dolares)'] = movies_df['revenue']
movies_df.drop('revenue', axis=1, inplace=True)

#ETL credits_df
credits_df['A'] = credits_df['crew'].apply(obtener)
credits_df.drop('cast', axis=1, inplace=True)
credits_df.drop('crew', axis=1, inplace=True)
credits_df['A'] = credits_df['A'].astype(str)
col = Desanidar(credits_df, 1)
credits_df = pd.concat([credits_df, col], axis= 1)
directors_df = credits_df[credits_df.columns[[0, 5]]]
directors_df.columns.values[1] = 'director'
directors_df['director'] = directors_df['director'].fillna('')
directors_df['director'] = directors_df['director'].apply(insertar_espacio)
#Merge por id
movies_df.loc[:, 'id'] = pd.to_numeric(movies_df['id'], errors='coerce')
directors_df.loc[:, 'id'] = pd.to_numeric(directors_df['id'], errors='coerce')
df = pd.merge(movies_df, directors_df, on='id', how='outer')

#/Unir todas las columnas desanidadas por su sub-categoria "name" en una sola columna.
df['belongs_to_collection: name'] = df['belongs_to_collection: name'].fillna('')
df['belongs_to_collection'] = df['belongs_to_collection: name'].apply(lambda fila: ''.join(filter(None, fila)))
df = df.drop(columns=['belongs_to_collection: name'])
df["belongs_to_collection"] = df["belongs_to_collection"].apply(lambda x: x.strip())
df['belongs_to_collection'] = df['belongs_to_collection'].apply(insertar_espacio)
df['belongs_to_collection'] = df['belongs_to_collection'].fillna('')

df['genres: name'] = df['genres: name'].fillna('')
df['genres'] = df['genres: name'].apply(lambda fila: ' - '.join(filter(None, fila)), axis=1)
df = df.drop(columns=['genres: name'])
df["genres"] = df["genres"].apply(lambda x: x.strip())
df["genres"] = df["genres"].fillna('')

df['production_companies: name'] = df['production_companies: name'].fillna('')
df['production_companies'] = df['production_companies: name'].apply(lambda fila: ' - '.join(filter(None, fila)), axis=1)
df = df.drop(columns=['production_companies: name'])
df["production_companies"] = df["production_companies"].apply(lambda x: x.strip())
df['production_companies'] = df['production_companies'].apply(insertar_espacio)
df['production_companies'] = df['production_companies'].fillna('')

df['production_countries: name'] = df['production_countries: name'].fillna('')
df['production_countries'] = df['production_countries: name'].apply(lambda fila: ' - '.join(filter(None, fila)), axis=1)
df = df.drop(columns=['production_countries: name'])
df["production_countries"] = df["production_countries"].apply(lambda x: x.strip())
df['production_countries'] = df['production_countries'].apply(insertar_espacio)
df['production_countries'] = df['production_countries'].fillna('')

df['spoken_languages: name'] = df['spoken_languages: name'].fillna('')
df['spoken_languages'] = df['spoken_languages: name'].apply(lambda fila: ' - '.join(filter(None, fila)), axis=1)
df = df.drop(columns=['spoken_languages: name'])
df["spoken_languages"] = df["spoken_languages"].apply(lambda x: x.strip())
df['spoken_languages'] = df['spoken_languages'].apply(insertar_espacio)
df['spoken_languages'] = df['spoken_languages'].fillna('')
#/

#Eliminar columnas con mas del 70% de datos nulos ya que dicha ausencia vuelve irrelevante la variable
df = eliminar_columnas_nulas(df, 70)

df.drop('vote_count', axis=1, inplace=True) #Se elimina la columna 'vote_count' considerada irrelevante ya que el puntaje de reseña no depende de la cantidad de votos sino de los resultados de los mismos
df.drop('id', axis=1, inplace=True)#Se elimina la columna 'id' ya que es redundante con el indice del dataset y no se utiliza como clave foranea.
df = df.drop_duplicates()
df = df.dropna(subset=['title'])

df.to_csv('etl_df', index= False)