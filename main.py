from fastapi import FastAPI
import pandas as pd
import numpy as np


app = FastAPI()

df = pd.read_csv('etl_df', low_memory= False)

@app.get("/peliculas_idioma/{idioma}")
def peliculas_idioma(idioma: str):
    cont = np.count_nonzero(df['original_language'] == idioma)
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
    '''Se ingresa la franquicia, retornando la cantidad de peliculas, ganancia total y promedio'''
    return {'franquicia':franquicia, 'cantidad':respuesta, 'ganancia_total':respuesta, 'ganancia_promedio':respuesta}

@app.get('/peliculas_pais/{pais}')
def peliculas_pais(pais:str):
    '''Ingresas el pais, retornando la cantidad de peliculas producidas en el mismo'''
    return {'pais':pais, 'cantidad':respuesta}

@app.get('/productoras_exitosas/{productora}')
def productoras_exitosas(productora:str):
    '''Ingresas la productora, entregandote el revunue total y la cantidad de peliculas que realizo '''
    return {'productora':productora, 'revenue_total': respuesta,'cantidad':respuesta}

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