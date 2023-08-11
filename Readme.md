Proyecto Individual Henry N° 1 (Machine learning operations) (Autor: Emanuel Duenk)

EXTRACCION - TRANSFORMACION - CARGA (ETL)
En un archivo .py se realizaron las siguientes acciones:

Extraccion

Se realizo la extraccion desde los archivos movies_dataset.csv y credits.csv, Para la misma
utilize una API de google drive para acceder a los archivos en linea directamente desde mi
cuenta de Drive, para eso es necesario instalar las bibliotecas: google-auth, google-auth-oauthlib, google-auth-httplib2, google-api-python-client y luego de descargar las credenciales desde la cuenta de google drive, al ejecutar el script quickstart.py se generara el token de acceso a los archivos.

Transformacion

Se realizaron las transformaciones requeridas:
-Desanidado de campos que contienen listas y diccionarios.
-Los valores nulos de los campos revenue, budget se rellenaron con el número 0.
-Los valores nulos del campo release date se eliminaron.
-El campo release_date se transformo a formato AAAA-mm-dd.
-Se creo la columna anio, donde se extrajo el año de release date.
-Se creo la columna return con el retorno de inversión, dividiendo revenue / budget.
-Se eliminaron las columnas, video,imdb_id,adult,original_title,poster_path y homepage.

Ademas se realizo:
-Extraccion de los nombres de los directores de la columna crew de credits csv utilizando la funcion re.findall.
-Merge del dataframe credits.csv reducido con los nombres de los directores con el df movies_dataset por el campo id.
-Al desanidar, se obtuvieron columnas con datos similares, se separaron solo los nombres (name) y se unieron utilizando separadores '-'.
-Se eliminaron todas las columnas con mas del 70% de valores nulos.
-Se eliminaron las columnas vote_count e id por considerarlas irrelevantes para las consultas a realizar.
-Se eliminaron filas duplicadas.
-Se renombraron los campor 'budget', 'revenue' y 'return' para expresar las unidades de exprecion (dolares y porcentajes)

Carga

-Se cargo el dataset transformado en el archivo etl.csv.

ANALISIS EXPLORATORIO DE DATOS (EDA) (Trabajo incompleto)
En un archivo .ipynb se realizaron las siguientes acciones:

Con el objetivo de entender los datos, se realizaron las siguientes acciones:
-Se extrajo la informacion general del dataset utilizando df.info()
-Se extrajeron los principales parametros estadisticos utilizando df.describe
-Se graficaron los campos 'budget', 'revenue' y 'return'

FUNCIONES

-En todas las funciones se transforman los datos respuesta a tipo de datos nativos de python para no tener errores al devolver el .json

SISTEMA DE RECOMENDACION

-Se solicita un titulo.
-Se obtiene el primer genero correspondiente a dicho titulo (hay varios generos en un mismo item). 
-Se reduce el dataframe solo a los items con generos similares. 
-Se eliminan columnas irrelevantes ('original_language', 'release_date', 'status', 'tagline', 'runtime_(minutos)','budget_(dolares)', 'revenue_(dolares)', 'production_companies', 'production_countries'). 
-Se aplica un split a las columnas 'overview' y 'spoken_languages'.
-Se transforma todo el dataframe a tipos de datos object.str.
-Se aplica label encoding al df.
-Se obtiene la matriz de similitud de coseno.
-El la fila correspondiente al valor buscado, se seleccionan los 6 valores de coseno mas alto que se guardan en un array.
-Se elimina el primer valor que es '1' el cual corresponde a la similitud entre filas similares (la fila con su misma fila).
-Se extraen los indices del array.
-Se extraen los titulos de las peliculas del dataframe en base a los indices, se guardan en una lista y se devuelve el dato.
IMPORTANTE: DEBIDO A LA LIMITACION DE RECURSOS DE RENDER, HAY CIERTOS TITULOS PARA LOS CUALES EL SISTEMA DE RECOMENDACION NO FUNCIONARA.

link deploy : https://proyecto-individual-peliculas-bct7.onrender.com/

