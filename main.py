from fastapi import FastAPI
import pandas as pd
import numpy as np


app = FastAPI()

df = pd.read_csv('etl_df', low_memory= False)

@app.get("/peliculas_idioma/{idioma}")
def peliculas_idioma(idioma: str):
    cont = df['original_language'].value_counts()[idioma]
    
    return {'idioma':idioma, 'cantidad':cont}
    
@app.get('/peliculas_duracion/{pelicula}')
def peliculas_duracion(titulo: str):
    dato = titulo.title()
    pelicula = df.loc[df.index[df['title'] == dato].tolist(), 'title'].iloc[0]
    duracion = df.loc[df.index[df['title'] == dato].tolist(), 'runtime_(minutos)'].iloc[0]
    anio = df.loc[df.index[df['title'] == dato].tolist(), 'anio'].iloc[0]

    return {'pelicula':pelicula, 'duracion':duracion, 'anio':anio}

@app.get('/franquicia/{franquicia}')
def franquicia(franquicia:str):
    dato = franquicia.title()
    df["belongs_to_collection"] = df["belongs_to_collection"].fillna('')
    franq = df[df["belongs_to_collection"].str.contains(dato, case=False)]
    cantidad = franq['belongs_to_collection'].count()
    ganancia_total = round(franq['revenue_(dolares)'].sum() - franq['budget_(dolares)'].sum())
    ganancia_promedio = round(franq['revenue_(dolares)'].mean() - franq['budget_(dolares)'].mean())

    return {'franquicia':dato, 'cantidad':cantidad, 'ganancia_total':ganancia_total, 'ganancia_promedio':ganancia_promedio}

@app.get('/peliculas_pais/{pais}')
def peliculas_pais(pais:str):
    dato = pais.title()
    df["production_countries"] = df["production_countries"].fillna('')
    countr = df[df["production_countries"].str.contains(dato, case=False)]
    respuesta = countr['production_countries'].count()

    return {'pais':dato, 'cantidad':respuesta}

@app.get('/productoras_exitosas/{productora}')
def productoras_exitosas(productora:str):
    dato = productora.title()
    df["production_companies"] = df["production_companies"].fillna('')
    prod = df[df["production_companies"].str.contains(dato, case=False)]
    revenue_total = round(prod['revenue_(dolares)'].sum())
    cantidad = prod['title'].count()

    return {'productora':dato, 'revenue_total': revenue_total,'cantidad':cantidad}

@app.get('/get_director/{nombre_director}')
def get_director(nombre_director:str):
    ''' Se ingresa el nombre de un director que se encuentre dentro de un dataset debiendo devolver el éxito del mismo medido a través del retorno. 
    Además, deberá devolver el nombre de cada película con la fecha de lanzamiento, retorno individual, costo y ganancia de la misma. En formato lista'''
    return {'director':nombre_director, 'retorno_total_director':respuesta, 
    'peliculas':respuesta, 'anio':respuesta,, 'retorno_pelicula':respuesta, 
    'budget_pelicula':respuesta, 'revenue_pelicula':respuesta}

# ML
@app.get('/recomendacion/{titulo}')
def recomendacion(titulo:str):
    '''Ingresas un nombre de pelicula y te recomienda las similares en una lista'''
    return {'lista recomendada': respuesta}