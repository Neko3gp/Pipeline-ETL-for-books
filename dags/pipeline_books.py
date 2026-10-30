

import json
import re
from datetime import datetime, timedelta
from statistics import mean
from urllib.parse import urljoin

import requests
from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator

# Dentro del Docker los servicios se resuelven por nombre de servicio,
# por esto el DAG no necesita usar localhost para comunicarse con Kafka o MongoDB.
KAFKA = "kafka:29092"
MONGO = "mongodb://mongodb:27017"

# Un fallo solamente reintentará reconectar 2 veces con un retraso de 30 segundos
args = {"retries": 2, "retry_delay": timedelta(seconds=30)}

# Books to Scrape devuelve el rating como texto; MongoDB guardara un numero
RATINGS = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}


def conectar_mongo(): #definir funcion conectar_mongo 
    """Devuelve la base de datos del pipeline usando la conexion de Docker."""
    from pymongo import MongoClient

    return MongoClient(MONGO)["books_pipeline"]


def leer_pagina(url): #definir funcion leer_pagina
    """Descarga una pagina y devuelve un arbol BeautifulSoup para consultarla."""
    from bs4 import BeautifulSoup #beautifulsoup4 es una libreria para analizar documentos HTML y XML de forma estructurada

    r = requests.get(url, timeout=15) #si tarda más de 15 segundos se genera una excepción de tiempo de espera
    r.raise_for_status()
    r.encoding = "utf-8"  # para que el símbolo £ se lea bien
    return BeautifulSoup(r.text, "html.parser")

##########################################
@dag(
    dag_id="pipeline_books",
    start_date=datetime(2026, 1, 1), #inicializamos el dag para que se ejecute a partir del 1 de enero de 2026
    schedule="@daily", 
    catchup=False,      #el dag se deja pensado para inicializarse diariamente, pero no se ejecuta para fechas pasadas
    default_args=args, 
    tags=["bigdata", "books", "kafka", "mongodb", "streamlit"], 
)
def pipeline():
    """Define el DAG y sus dependencias; Airflow ejecuta las tareas internas."""

    inicio = EmptyOperator(task_id="start") #estos operadores no hacen nada, solo sirven para marcar el inicio y fin del DAG
    fin = EmptyOperator(task_id="end")

    # 1 API: Open Library Esta fuente aporta metadatos bibliograficos mas no precio ni rating y se limita a libros de ficción
    @task
    def extraer_openlibrary_api():
        r = requests.get(
            "https://openlibrary.org/search.json",
            params={
                "q": "subject:fiction",
                "limit": 100, #limita la cantidad de libros a 100 para no saturar la API y mantener el DAG rápido
                "fields": "key,title,author_name,first_publish_year,subject", #campos que se desean obtener de la API para cada libro
            },
            timeout=30, #si tarda más de 30secs se genera execpcion de tiempo de espera
        )
        r.raise_for_status() 
        libros = r.json()["docs"] 
        print(f"Open Library: {len(libros)} libros") #imprime la cantidad de libros obtenidos
        return libros

    # 2 Scraping: Books to Scrape aporta precio y rating pero no metadatos bibliograficos
    @task
    def scraping_books_to_scrape():
        base = "https://books.toscrape.com/" 
        sopa = leer_pagina(base) 

        # La barra lateral contiene las categorias y sus enlaces 
        categorias = [(a.text.strip(), urljoin(base, a["href"])) for a in sopa.select("div.side_categories ul ul a")] #extrae el nombre de la categoria y la url de cada categoria
        if not categorias:
            raise ValueError("No se encontraron categorías: revisa el HTML de la página") # Si no hay categorías, se lanza una excepción

        libros = [] #aqui se almacenan los libros obtenidos del scraping con todos sus datos
        for nombre, url in categorias:
            while url:  
                sopa = leer_pagina(url)
                for libro in sopa.select("article.product_pod"):
                    libros.append({
                        "titulo": libro.h3.a["title"],
                        "precio": libro.select_one("p.price_color").text,                 
                        "rating": libro.select_one("p.star-rating")["class"][1],          
                        "disponibilidad": libro.select_one("p.availability").text.strip(),  #saber si hay stock
                        "categoria": nombre, 
                    })
                siguiente = sopa.select_one("li.next a") # Busca el enlace a la siguiente página para continuar el scraping
                url = urljoin(url, siguiente["href"]) if siguiente else None # Si hay siguiente, se actualiza la url, si no, se pone None

        print(f"Books to Scrape: {len(libros)} libros en {len(categorias)} categorías")
        return libros

    # 3 Publicar en Kafka. Cada origen tiene un topic independiente.
    @task
    def enviar_books_kafka(libros_api, libros_scraping):
        from kafka import KafkaProducer

        producer = KafkaProducer( #el producer se encarga de enviar los mensajes a Kafka
            bootstrap_servers=KAFKA,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        for libro in libros_api: #ciclo for para enviar cada libro a su respectivo topic en Kafka
            producer.send("books_api", libro)
        for libro in libros_scraping:
            producer.send("books_scraping", libro)
        producer.flush() #se flushea el buffer para asegurarse de que todos los mensajes se envíen antes de cerrarse
        producer.close()
        print(f"Enviados a Kafka: {len(libros_api)} a books_api y {len(libros_scraping)} a books_scraping") #si todo sale bien, se imprime la cantidad de libros enviados a cada topico

    # 4 Consumir de Kafka y transformar al esquema que usara MongoDB.
    @task
    def consumo_transformacion_books():
        from kafka import KafkaConsumer

        # El consumer se encarga de recibir los mensajes de Kafka y transformarlos al esquema que usara MongoDB.
        consumer = KafkaConsumer(
            "books_api", "books_scraping",# se unen tanto la api como el scraping para consumir los mensajes de ambos topicos
            bootstrap_servers=KAFKA, 
            value_deserializer=lambda v: json.loads(v.decode("utf-8")), # se deserializa el valor del mensaje de JSON a Python
            group_id="airflow-books",        # Kafka conserva el offset del consumidor.
            auto_offset_reset="earliest",
            consumer_timeout_ms=20000,       # Termina tras 20 s sin mensajes nuevos.
        )
# se cierra el consumer y se imprime un mensaje de confirmación
        api, scraped = [], [] # se inicializan las listas para almacenar los datos transformados
        for mensaje in consumer:
            d = mensaje.value
            if mensaje.topic == "books_api": #si el mensaje proviene del topic de la API, se transforma al esquema de MongoDB
                subjects = d.get("subject", [])
                api.append({
                    "titulo": d["title"],
                    "autor": ", ".join(d.get("author_name", [])) or "Desconocido",
                    "año": d.get("first_publish_year"),
                    "categoria": subjects[0] if subjects else "Sin categoría",
                    "temas": subjects[:5],
                    "olid": d["key"],
                })
            else: #si no, el mensaje proviene del topic de scraping, se transforma al esquema de MongoDB
                # Transformar los datos del scraping al esquema de MongoDB
                scraped.append({
                    "titulo": d["titulo"].strip(),
                    "precio": float(re.sub(r"[^0-9.]", "", d["precio"])),  # "£51.77" -> 51.77
                    "rating": RATINGS[d["rating"]],                        # "Three" -> 3
                    "disponible": "In stock" in d["disponibilidad"],       # texto -> True/False
                    "categoria": d["categoria"].strip(),
                })
        consumer.commit() #hacemos commit del offset para que Kafka sepa que ya se procesaron los mensajes
        consumer.close()
        print(f"Transformados: {len(api)} de la API y {len(scraped)} del scraping") 
        return {"api": api, "scraped": scraped} #queda la lista de diccionarios con los datos transformados para ser cargados en MongoDB

    # 5 Cargar en MongoDB. Se reemplaza el resultado anterior de forma controlada.
    @task
    def cargar_books_mongodb(datos):
        db = conectar_mongo()
        # La recarga completa hace el DAG testigo respecto a MongoDB: repetirlo
        # no acumula duplicados, aunque cada ejecucion vuelva a obtener los origenes.
        db.books_api.delete_many({})
        db.books_scraped.delete_many({})

        if datos["api"]: #si hay datos de la API, se insertan en la colección books_api
            db.books_api.insert_many(datos["api"])

        if datos["scraped"]: #si hay datos del scraping, se insertan en la colección books_scraped
            db.books_scraped.insert_many(datos["scraped"])
        print(f"Guardados en MongoDB: {len(datos['api'])} en books_api y {len(datos['scraped'])} en books_scraped")

    # 6 Calcular metricas para el dashboard.
    @task
    def calcular_metricas_books(): #comenzamos a calcular las métricas de los libros para el dashboard
        db = conectar_mongo()
        libros = list(db.books_scraped.find({}, {"_id": 0})) #obtenemos todos los libros del scraping
        if not libros:
            raise ValueError("No hay libros en books_scraped")

        precios = [l["precio"] for l in libros] #precios de todos los libros y todos los ratings de todos los libros
        ratings = [l["rating"] for l in libros]

        # Agrupar precios permite mostrar el promedio y el volumen por categoria.
        por_categoria = {}
        for l in libros:
            por_categoria.setdefault(l["categoria"], []).append(l["precio"]) #agregamos el precio de cada libro a su categoría

        metricas = {
            "total_libros_scraped": len(libros), 
            "total_libros_api": db.books_api.count_documents({}),#contamos todos los libros en la colección de la api
            "precio_promedio": round(mean(precios), 2), 
            "precio_minimo": min(precios),
            "precio_maximo": max(precios),
            "rating_promedio": round(mean(ratings), 2),
            "libros_por_categoria": [
                {"categoria": c, "total": len(p), "precio_promedio": round(mean(p), 2)} #cantidad y promedio de precios por categoria
                for c, p in sorted(por_categoria.items())
            ],
            "distribucion_ratings": [{"rating": r, "total": ratings.count(r)} for r in range(1, 6)], #distribución de ratings de 1 a 5 estrellas
            "calculado_en": datetime.now().isoformat(timespec="seconds"), #momento en que se calculan las métricas
        }
        db.books_metricas.delete_many({}) #eliminamos las métricas anteriores para no acumular duplicados
        db.books_metricas.insert_one(metricas) #insertamos las métricas calculadas en la colección books_metricas
        print(f"Métricas: precio promedio £{metricas['precio_promedio']}, rating promedio {metricas['rating_promedio']}")

    # 7 Marcar la actualizacion del dashboard.
    @task
    def actualizar_dashboard_streamlit():
        # Streamlit consulta MongoDB al abrirse o recargarse; no necesita que
        # Airflow le envie datos. Solo registramos cuando termino el pipeline.
        db = conectar_mongo()
        ahora = datetime.now().isoformat(timespec="seconds")
        db.books_metricas.update_one({}, {"$set": {"dashboard_actualizado_en": ahora}})#actualizamos el campo dashboard_actualizado_en con la fecha y hora actual
        print(f"Dashboard listo ({ahora}): abre http://localhost:8501")

    # Dependencias: las dos extracciones pueden ejecutarse en paralelo; el resto
    # conserva un orden lineal porque cada etapa necesita la salida anterior.
    api = extraer_openlibrary_api()
    scraping = scraping_books_to_scrape() 
    kafka = enviar_books_kafka(api, scraping)
    consumo = consumo_transformacion_books()
    cargar = cargar_books_mongodb(consumo)
    metricas = calcular_metricas_books()
    dashboard = actualizar_dashboard_streamlit()

    inicio >> [api, scraping] >> kafka >> consumo >> cargar >> metricas >> dashboard >> fin
#inicia, extrae los datos, los envia a kafka, los consume y transforma, los carga en mongodb, calcula las métricas y actualiza el dashboard, y finalmente termina el DAG

pipeline()
