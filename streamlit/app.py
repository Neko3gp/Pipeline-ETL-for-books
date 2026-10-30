import numpy as np
import pandas as pd
import streamlit as st
from pymongo import MongoClient

 #configuración de la página de Streamlit y título del dashboard
st.set_page_config(page_title="Pipeline Books", layout="wide") 
st.title("Pipeline Books") 

# MongoDB es la fuente única del dashboard. El contenedor de Streamlit
# alcanza al contenedor de MongoDB por su nombre dentro de Docker Compose.

db = MongoClient("mongodb://mongodb:27017")["books_pipeline"] #conectamos a la base de datos
libros = pd.DataFrame(list(db.books_scraped.find({}, {"_id": 0})))#obtenemos todos los libros del scraping
api = pd.DataFrame(list(db.books_api.find({}, {"_id": 0})))#obtenemos todos los libros de la API
metricas = db.books_metricas.find_one({}, {"_id": 0})#obtenemos las métricas calculadas


#hasta que no haya datos, se queda en espera
if libros.empty or metricas is None: 
    st.info("Todavía no hay datos. Ejecuta el DAG pipeline_books en Airflow (http://localhost:8080).")
    st.stop()

st.caption(f"Datos actualizados: {metricas.get('dashboard_actualizado_en', metricas['calculado_en'])}") 


######################################
# Resumen de las metricas calculadas por la tarea calcular_metricas_books.
c1, c2, c3, c4 = st.columns(4)
c1.metric("Libros (Books to Scrape)", metricas["total_libros_scraped"])
c2.metric("Libros (Open Library)", metricas["total_libros_api"])
c3.metric("Precio promedio", f"£{metricas['precio_promedio']}")
c4.metric("Rating promedio", metricas["rating_promedio"])
#############################
# 1. Histograma de precios.
st.subheader("Histograma de precios")
cuentas, bordes = np.histogram(libros["precio"], bins=10)
histograma = pd.DataFrame({"precio desde (£)": [f"{b:05.1f}" for b in bordes[:-1]], "libros": cuentas})
st.bar_chart(histograma, x="precio desde (£)", y="libros")

###################
# 2. Volumen de libros por categoria.
st.subheader("Libros por categoría")
por_categoria = pd.DataFrame(metricas["libros_por_categoria"])
st.bar_chart(por_categoria, x="categoria", y="total")

#####################
# 3. Distribucion de ratings.
st.subheader("Distribución de ratings")
ratings = pd.DataFrame(metricas["distribucion_ratings"])
ratings["rating"] = ratings["rating"].astype(str) + " estrellas"
st.bar_chart(ratings, x="rating", y="total")


############################
# 4 y 5. Rankings: se separan en dos columnas para compararlos.
izquierda, derecha = st.columns(2)
with izquierda:
    st.subheader("Top 10 libros más caros")
    st.dataframe(libros.nlargest(10, "precio")[["titulo", "categoria", "precio"]], hide_index=True)

with derecha:
    st.subheader("Top 10 libros mejor calificados")
    st.caption("Muchos libros tienen 5 estrellas; se desempata por precio.")
    mejores = libros.sort_values(["rating", "precio"], ascending=False).head(10)
    st.dataframe(mejores[["titulo", "categoria", "rating", "precio"]], hide_index=True)


########################
# 6. Tabla unificada y filtros. Las columnas que no existen en un origen
# quedan vacias, porque cada fuente aporta datos diferentes.

st.subheader("Todos los libros")
tabla = pd.concat([
    libros.assign(fuente="Books to Scrape"),
    api.assign(fuente="Open Library"),
])[["fuente", "titulo", "autor", "categoria", "precio", "rating", "año"]]

f1, f2, f3 = st.columns(3) #columnas para los filtros de búsqueda
titulo = f1.text_input("Filtrar por título")
autor = f2.text_input("Filtrar por autor")
categoria = f3.text_input("Filtrar por categoría")

for columna, texto in (("titulo", titulo), ("autor", autor), ("categoria", categoria)):
    if texto:
        tabla = tabla[tabla[columna].str.contains(texto, case=False, regex=False, na=False)]

st.caption("Los autores vienen de Open Library; los precios y ratings, de Books to Scrape.") #los datos de la API no incluyen precio ni rating
st.dataframe(tabla, hide_index=True)                                                         #por eso aparecen vacíos en la tabla unificada
