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
movies_df.dropna(subset=['release_date'], inplace=True)
movies_df['release_date'] = pd.to_datetime(movies_df['release_date'], format= '%Y-%m-%d', errors= 'coerce')
movies_df['return'] = movies_df['revenue']/movies_df['budget']
movies_df['return'] = movies_df['return'].fillna(0)

#ETL credits_df
credits_df['A'] = credits_df['crew'].apply(obtener)
credits_df.drop('cast', axis=1, inplace=True)
credits_df.drop('crew', axis=1, inplace=True)
credits_df['A'] = credits_df['A'].astype(str)
col = Desanidar(credits_df, 1)
credits_df = pd.concat([credits_df, col], axis= 1)
directors_df = credits_df[credits_df.columns[[0, 5]]]
directors_df.columns.values[1] = 'director'

#Merge por id
movies_df.loc[:, 'id'] = pd.to_numeric(movies_df['id'], errors='coerce')
directors_df.loc[:, 'id'] = pd.to_numeric(directors_df['id'], errors='coerce')
df = pd.merge(movies_df, directors_df, on='id', how='outer')

#/Unir todas las columnas desanidadas por su sub-categoria "name" en una sola columna.
df['genres: name'] = df['genres: name'].fillna('')
df['genres'] = df['genres: name'].apply(lambda fila: ' - '.join(filter(None, fila)), axis=1)
df = df.drop(columns=['genres: name'])
df['genres'] = df['genres'].str.replace(' ', '')

df['production_companies: name'] = df['production_companies: name'].fillna('')
df['production_companies'] = df['production_companies: name'].apply(lambda fila: ' - '.join(filter(None, fila)), axis=1)
df = df.drop(columns=['production_companies: name'])
df['production_companies'] = df['production_companies'].str.replace(' ', '')

df['production_countries: name'] = df['production_countries: name'].fillna('')
df['production_countries'] = df['production_countries: name'].apply(lambda fila: ' - '.join(filter(None, fila)), axis=1)
df = df.drop(columns=['production_countries: name'])
df['production_countries'] = df['production_countries'].str.replace(' ', '')

df['spoken_languages: name'] = df['spoken_languages: name'].fillna('')
df['spoken_languages'] = df['spoken_languages: name'].apply(lambda fila: ' - '.join(filter(None, fila)), axis=1)
df = df.drop(columns=['spoken_languages: name'])
df['spoken_languages'] = df['spoken_languages'].str.replace(' ', '')
#/

#Eliminar columnas con mas del 70% de datos nulos ya que dicha ausencia vuelve irrelevante la variable
df = eliminar_columnas_nulas(df, 70)

df.drop('vote_count', axis=1, inplace=True) #Se elimina la columna 'vote_count' considerada irrelevante ya que el puntaje de reseña no depende de la cantidad de votos sino de los resultados de los mismos
df.drop('id', axis=1, inplace=True)#Se elimina la columna 'id' ya que es redundante con el indice del dataset y no se utiliza como clave foranea.

df.to_csv('etl_df', index= False)