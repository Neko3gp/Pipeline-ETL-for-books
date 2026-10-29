"""DAG de Airflow que implementa un pipeline ETL de libros.

El flujo combina dos fuentes con formatos distintos, usa Kafka como capa de
mensajeria y persiste los resultados transformados en MongoDB. Streamlit
consulta MongoDB para presentar las metricas y los libros procesados.
"""

import json
import re
from datetime import datetime, timedelta
from statistics import mean
from urllib.parse import urljoin

import requests
from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator

# Dentro de la red de Docker los servicios se resuelven por nombre de servicio,
# por eso el DAG no debe usar localhost para comunicarse con Kafka o MongoDB.
KAFKA = "kafka:29092"
MONGO = "mongodb://mongodb:27017"

# Un fallo transitorio de red no debe detener el pipeline definitivamente.
args = {"retries": 2, "retry_delay": timedelta(seconds=30)}

# Books to Scrape devuelve el rating como texto; MongoDB guardara un numero.
RATINGS = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}


def conectar_mongo():
    """Devuelve la base de datos del pipeline usando la conexion de Docker."""
    from pymongo import MongoClient

    return MongoClient(MONGO)["books_pipeline"]


def leer_pagina(url):
    """Descarga una pagina y devuelve un arbol BeautifulSoup para consultarla."""
    from bs4 import BeautifulSoup

    r = requests.get(url, timeout=15)
    r.raise_for_status()
    r.encoding = "utf-8"  # para que el símbolo £ se lea bien
    return BeautifulSoup(r.text, "html.parser")


@dag(
    dag_id="pipeline_books",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=args,
    tags=["bigdata", "books", "kafka", "mongodb", "streamlit"],
)
def pipeline():
    """Define el DAG y sus dependencias; Airflow ejecuta las tareas internas."""

    inicio = EmptyOperator(task_id="start")
    fin = EmptyOperator(task_id="end")

    # 1. API: Open Library. Esta fuente aporta metadatos bibliograficos.
    @task
    def extraer_openlibrary_api():
        r = requests.get(
            "https://openlibrary.org/search.json",
            params={
                "q": "subject:fiction",
                "limit": 100,
                "fields": "key,title,author_name,first_publish_year,subject",
            },
            timeout=30,
        )
        r.raise_for_status()
        libros = r.json()["docs"]
        print(f"Open Library: {len(libros)} libros")
        return libros

    # 2. Scraping: Books to Scrape. Esta fuente aporta precio y rating.
    @task
    def scraping_books_to_scrape():
        base = "https://books.toscrape.com/"
        sopa = leer_pagina(base)

        # La barra lateral contiene las categorias y sus enlaces relativos.
        categorias = [(a.text.strip(), urljoin(base, a["href"])) for a in sopa.select("div.side_categories ul ul a")]
        if not categorias:
            raise ValueError("No se encontraron categorías: revisa el HTML de la página")

        libros = []
        for nombre, url in categorias:
            while url:  # Recorre todas las paginas de la categoria.
                sopa = leer_pagina(url)
                for libro in sopa.select("article.product_pod"):
                    libros.append({
                        "titulo": libro.h3.a["title"],
                        "precio": libro.select_one("p.price_color").text,                 # "£51.77"
                        "rating": libro.select_one("p.star-rating")["class"][1],          # "Three"
                        "disponibilidad": libro.select_one("p.availability").text.strip(),  # "In stock"
                        "categoria": nombre,
                    })
                siguiente = sopa.select_one("li.next a")
                url = urljoin(url, siguiente["href"]) if siguiente else None

        print(f"Books to Scrape: {len(libros)} libros en {len(categorias)} categorías")
        return libros

    # 3. Publicar en Kafka. Cada origen tiene un topic independiente.
    @task
    def enviar_books_kafka(libros_api, libros_scraping):
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=KAFKA,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        for libro in libros_api:
            producer.send("books_api", libro)
        for libro in libros_scraping:
            producer.send("books_scraping", libro)
        producer.flush()
        producer.close()
        print(f"Enviados a Kafka: {len(libros_api)} a books_api y {len(libros_scraping)} a books_scraping")

    # 4. Consumir de Kafka y transformar al esquema que usara MongoDB.
    @task
    def consumo_transformacion_books():
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            "books_api", "books_scraping",
            bootstrap_servers=KAFKA,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="airflow-books",        # Kafka conserva el offset del consumidor.
            auto_offset_reset="earliest",
            consumer_timeout_ms=20000,       # Termina tras 20 s sin mensajes nuevos.
        )

        api, scraped = [], []
        for mensaje in consumer:
            d = mensaje.value
            if mensaje.topic == "books_api":
                subjects = d.get("subject", [])
                api.append({
                    "titulo": d["title"],
                    "autor": ", ".join(d.get("author_name", [])) or "Desconocido",
                    "anio": d.get("first_publish_year"),
                    "categoria": subjects[0] if subjects else "Sin categoría",
                    "temas": subjects[:5],
                    "olid": d["key"],
                })
            else:
                scraped.append({
                    "titulo": d["titulo"].strip(),
                    "precio": float(re.sub(r"[^0-9.]", "", d["precio"])),  # "£51.77" -> 51.77
                    "rating": RATINGS[d["rating"]],                        # "Three" -> 3
                    "disponible": "In stock" in d["disponibilidad"],       # texto -> True/False
                    "categoria": d["categoria"].strip(),
                })
        consumer.commit()
        consumer.close()
        print(f"Transformados: {len(api)} de la API y {len(scraped)} del scraping")
        return {"api": api, "scraped": scraped}

    # 5. Cargar en MongoDB. Se reemplaza el resultado anterior de forma controlada.
    @task
    def cargar_books_mongodb(datos):
        db = conectar_mongo()
        # La recarga completa hace el DAG idempotente respecto a MongoDB: repetirlo
        # no acumula duplicados, aunque cada ejecucion vuelva a obtener los origenes.
        db.books_api.delete_many({})
        db.books_scraped.delete_many({})
        if datos["api"]:
            db.books_api.insert_many(datos["api"])
        if datos["scraped"]:
            db.books_scraped.insert_many(datos["scraped"])
        print(f"Guardados en MongoDB: {len(datos['api'])} en books_api y {len(datos['scraped'])} en books_scraped")

    # 6. Calcular metricas para el dashboard.
    @task
    def calcular_metricas_books():
        db = conectar_mongo()
        libros = list(db.books_scraped.find({}, {"_id": 0}))
        if not libros:
            raise ValueError("No hay libros en books_scraped")

        precios = [l["precio"] for l in libros]
        ratings = [l["rating"] for l in libros]

        # Agrupar precios permite mostrar el promedio y el volumen por categoria.
        por_categoria = {}
        for l in libros:
            por_categoria.setdefault(l["categoria"], []).append(l["precio"])

        metricas = {
            "total_libros_scraped": len(libros),
            "total_libros_api": db.books_api.count_documents({}),
            "precio_promedio": round(mean(precios), 2),
            "precio_minimo": min(precios),
            "precio_maximo": max(precios),
            "rating_promedio": round(mean(ratings), 2),
            "libros_por_categoria": [
                {"categoria": c, "total": len(p), "precio_promedio": round(mean(p), 2)}
                for c, p in sorted(por_categoria.items())
            ],
            "distribucion_ratings": [{"rating": r, "total": ratings.count(r)} for r in range(1, 6)],
            "calculado_en": datetime.now().isoformat(timespec="seconds"),
        }
        db.books_metricas.delete_many({})
        db.books_metricas.insert_one(metricas)
        print(f"Métricas: precio promedio £{metricas['precio_promedio']}, rating promedio {metricas['rating_promedio']}")

    # 7. Marcar la actualizacion del dashboard.
    @task
    def actualizar_dashboard_streamlit():
        # Streamlit consulta MongoDB al abrirse o recargarse; no necesita que
        # Airflow le envie datos. Solo registramos cuando termino el pipeline.
        db = conectar_mongo()
        ahora = datetime.now().isoformat(timespec="seconds")
        db.books_metricas.update_one({}, {"$set": {"dashboard_actualizado_en": ahora}})
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


pipeline()
