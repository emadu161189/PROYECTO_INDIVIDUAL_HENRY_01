import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics.pairwise import cosine_similarity
from fastapi import FastAPI
import uvicorn

app = FastAPI()

df = pd.read_csv('etl_df', low_memory= False)

@app.get("/peliculas_idioma/{idioma}")
def peliculas_idioma(idioma: str):
    cont = df['original_language'].value_counts()[idioma]
    cont = int(cont)

    return {'idioma':idioma, 'cantidad':cont}
    
@app.get('/peliculas_duracion/{titulo}')
def peliculas_duracion(titulo: str):
    dato = titulo.title()
    df["title"] = df["title"].fillna('')
    tit = df[df["title"].str.contains(dato, case=False)]
    tit_name = tit['title'].iloc[0]
    tit_name = str(tit_name)
    duracion = tit['runtime_(minutos)'].iloc[0]
    duracion = int(duracion)
    anio = tit['anio'].iloc[0]
    anio = int(anio)

    return {'pelicula':tit_name, 'duracion':duracion, 'anio':anio}

@app.get('/franquicia/{franquicia}')
def franquicia(franquicia:str):
    dato = franquicia.title()
    df["belongs_to_collection"] = df["belongs_to_collection"].fillna('')
    franq = df[df["belongs_to_collection"].str.contains(dato, case=False)]
    franquicia_name = franq['belongs_to_collection'].iloc[0]
    franquicia_name = str(franquicia_name)
    cantidad = franq['belongs_to_collection'].count()
    cantidad = int(cantidad)
    ganancia_total = round(franq['revenue_(dolares)'].sum() - franq['budget_(dolares)'].sum())
    ganancia_total = int(ganancia_total)
    ganancia_promedio = round(franq['revenue_(dolares)'].mean() - franq['budget_(dolares)'].mean())
    ganancia_promedio = int(ganancia_promedio)
    if franq.empty:
       error = {'error': "f'{dato} parametro incorrecto"}
       return error
    
    return {'franquicia':franquicia_name, 'cantidad':cantidad, 'ganancia_total':ganancia_total, 'ganancia_promedio':ganancia_promedio}

@app.get('/peliculas_pais/{pais}')
def peliculas_pais(pais:str):
    dato = pais.title()
    df["production_countries"] = df["production_countries"].fillna('')
    countr = df[df["production_countries"].str.contains(dato, case=False)]
    respuesta = countr['production_countries'].count()
    respuesta = int(respuesta)
    if countr.empty:
       error = {'error': "f'{dato} parametro incorrecto"}
       return error

    return {'pais':dato, 'cantidad':respuesta}

@app.get('/productoras_exitosas/{productora}')
def productoras_exitosas(productora:str):
    dato = productora.title()
    df["production_companies"] = df["production_companies"].fillna('')
    prod = df[df["production_companies"].str.contains(dato, case=False)]
    revenue_total = round(prod['revenue_(dolares)'].sum())
    revenue_total = int(revenue_total)
    cantidad = prod['title'].count()
    cantidad = int(cantidad)
    if prod.empty:
       error = {'error': "f'{dato} parametro incorrecto"}
       return error

    return {'productora':dato, 'revenue_total': revenue_total,'cantidad':cantidad}

@app.get('/get_director/{nombre_director}')
def get_director(nombre_director:str):
    dato = nombre_director.title()
    dir = df[df['director'] == dato]
    retorno_total_director = dir['revenue_(dolares)'].sum()
    retorno_total_director = float(retorno_total_director)
    peliculas = dir['title'].unique()
    peliculas = list(peliculas)
    anio = dir['anio']
    anio = list(anio)
    retorno_pelicula = dir['return_(%)']
    retorno_pelicula = list(retorno_pelicula)
    budget_pelicula = dir['budget_(dolares)']
    budget_pelicula = list(budget_pelicula)
    revenue_pelicula = dir['revenue_(dolares)']
    revenue_pelicula = list(revenue_pelicula)
    if dir.empty:
       error = {'error': "f'{dato} parametro incorrecto"}
       return error
    return {'director':dato, 'retorno_total_director':retorno_total_director, 
    'peliculas':peliculas, 'anio':anio, 'retorno_pelicula':retorno_pelicula, 
    'budget_pelicula':budget_pelicula, 'revenue_pelicula':revenue_pelicula}

# ML
@app.get('/recomendacion/{titulo}')
def recomendacion(titulo:str):
    dato = titulo.title()
    #Extraer el titulo y el genero
    primer_coincidencia_df = df[df["title"].str.contains(dato, case=False)]
    if primer_coincidencia_df.empty:
        error = {'error': "f'{dato} parametro incorrecto"}
        return error 

    titulo = primer_coincidencia_df['title'].iloc[0]
    genero = primer_coincidencia_df['genres'].iloc[0]
    sep = genero.find(' ')
    genero = genero[:sep]

    #Reducir el df a los que contengan el genero
    df['genres'] = df['genres'].fillna('')
    df_reducido = df[df["genres"].str.contains(genero, case=False)]
    
    #Eliminar columnas que no utilizaremos
    col_eliminar = ['original_language', 'release_date', 'status', 'tagline', 'runtime_(minutos)',
                    'budget_(dolares)', 'revenue_(dolares)', 'production_companies', 'production_countries']
    df_reducido.drop(columns= col_eliminar, inplace=True)

    #Splitear overview
    sp_over = df_reducido['overview'].str.split(pat= ' ', n= -1, expand= True)
    df_reducido = pd.concat([df_reducido, sp_over], axis= 1)
    df_reducido.drop(columns='overview', inplace= True)

    #Splitear spoken_languages
    sp_lang = df_reducido['spoken_languages'].str.split(pat= '-', n= -1, expand= True)
    df_reducido = pd.concat([df_reducido, sp_lang], axis= 1)
    df_reducido.drop(columns='spoken_languages', inplace= True)

    #Separar la columna title, obtener el indice del dato de entrada y eliminar la columna title del df reducido al cual se aplicara encoding
    lista_title = df_reducido['title'] #El indice de la variable X es [0]
    df_reducido.drop(columns='title', inplace= True)
    
    #Convertir todo df_reducido a tipo de dato string
    df_reducido = df_reducido.astype(str)

    #Aplicar label encoding a df_reducido
    label_encoder = LabelEncoder()
    df_encoded = df_reducido.apply(label_encoder.fit_transform)

    #Similitud de coseno a df_encoded
    similitud = cosine_similarity(df_encoded)

    #Debido a que el indice del dato de entrada es 0, separamos la primera fila de la matriz en un array

    array_similitud = similitud[0]

    #Obtener los indices de los 6 valores mas altos
    indices = np.argsort(-array_similitud)[:6]

    #Creamos una lista vacia 'respuesta', iteramos los indices y agregamos el valor de lista_title para cada indice, quitandole el indice [0], esas seran las peliculas recomendadas.
    respuesta = []

    for elemento in indices:
        respuesta.append(lista_title.iloc[elemento])

    respuesta = respuesta[1:]

    return {'lista recomendada': respuesta}


if __name__ == "__main__":
        uvicorn.run(app)
