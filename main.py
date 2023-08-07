from fastapi import FastAPI
import pandas as pd
import numpy as np


app = FastAPI()

df = pd.read_csv('etl_df', low_memory= False)

@app.get("/contar/{idioma}")
def peliculas_idioma(idioma: str):
    cont = np.count_nonzero(df['original_language'] == idioma)
    mensaje = f"{cont} películas se estrenaron en {idioma}"
    return mensaje 

