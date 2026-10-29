# Pipeline Books

Pipeline de datos reproducible que integra **Apache Airflow**, **Kafka**,
**MongoDB** y **Streamlit**. El proyecto extrae libros desde dos fuentes,
transporta los eventos por Kafka, normaliza sus formatos, los almacena en
MongoDB y presenta los resultados en un dashboard.

Está pensado para demostración y estudio: cada componente tiene una
responsabilidad clara y todo el entorno se levanta con Docker Compose.

## Arquitectura

```text
Open Library API ───────┐
                        ├─> Airflow ─> Kafka ─> transformación ─> MongoDB ─> Streamlit
Books to Scrape ───────┘
```

### Fuentes

- **Open Library** (`subject:fiction`, hasta 100 resultados): título, autor,
  año de publicación, temas e identificador `olid`.
- **Books to Scrape**: título, precio, rating, disponibilidad y categoría.

Los orígenes no comparten exactamente el mismo esquema. Por eso se conservan
en colecciones separadas y el dashboard identifica el origen mediante la
columna `fuente`.

### Servicios y puertos

| Servicio | Función | Acceso local |
|---|---|---|
| Airflow | Orquestación y ejecución del DAG | [localhost:8080](http://localhost:8080) |
| Kafka | Mensajería entre extracción y transformación | `localhost:9092` |
| MongoDB | Persistencia de libros y métricas | `localhost:27017` |
| Streamlit | Visualización de resultados | [localhost:8501](http://localhost:8501) |

Dentro de Docker, Airflow usa `kafka:29092` y `mongodb:27017`; `localhost`
solo se usa desde el equipo anfitrión.

## Requisitos

- Docker Desktop con Docker Compose.
- Conexión a Internet para consultar las fuentes públicas.
- Puertos `8080`, `8501`, `9092`, `2181` y `27017` disponibles.

No es necesario instalar Python, Airflow ni Kafka en el equipo: las imágenes
de Docker y las dependencias del proyecto se preparan al arrancar.

## Ejecución

Desde la raíz del proyecto:

```bash
docker compose up -d
```

La primera ejecución puede tardar unos minutos porque Airflow y Streamlit
instalan sus dependencias. Después:

1. Abre Airflow en <http://localhost:8080>.
2. Busca el DAG `pipeline_books`. Está configurado para ejecutarse
   automáticamente al iniciar y después una vez al día.
3. Usa **Trigger DAG** para lanzarlo manualmente si lo necesitas.
4. Espera a que todas las tareas aparezcan en verde.
5. Abre o recarga Streamlit en <http://localhost:8501>.

Para detener los servicios:

```bash
docker compose down
```

Para consultar MongoDB:

```bash
docker compose exec mongodb mongosh books_pipeline --eval \
  "db.books_api.countDocuments(); db.books_scraped.countDocuments(); db.books_metricas.findOne()"
```

## Flujo del DAG

`start` → extracciones en paralelo → publicación en Kafka → consumo y
transformación → carga en MongoDB → métricas → actualización de timestamp →
`end`.

| Tarea | Responsabilidad |
|---|---|
| `extraer_openlibrary_api` | Descarga metadatos bibliográficos de Open Library. |
| `scraping_books_to_scrape` | Recorre categorías y páginas de Books to Scrape. |
| `enviar_books_kafka` | Publica cada origen en `books_api` o `books_scraping`. |
| `consumo_transformacion_books` | Convierte textos a tipos útiles y unifica nombres de campos. |
| `cargar_books_mongodb` | Reemplaza las colecciones con el resultado actual. |
| `calcular_metricas_books` | Calcula agregados para el dashboard. |
| `actualizar_dashboard_streamlit` | Registra la hora en que el procesamiento terminó. |

La extracción de API y scraping son paralelas. El resto es secuencial porque
cada etapa depende de la salida de la anterior.

## Datos almacenados

- `books_api`: libros normalizados de Open Library.
- `books_scraped`: libros normalizados de Books to Scrape.
- `books_metricas`: un documento con promedios, mínimos, máximos, distribución
  de ratings y totales por categoría.

La carga elimina el contenido anterior y vuelve a insertar el resultado. Esta
decisión hace que repetir el DAG no duplique documentos en MongoDB y facilita
las demostraciones. No es una estrategia de histórico: para conservar
versiones habría que añadir una fecha de ejecución y usar upserts o una
colección histórica.

## Qué muestra el dashboard

- Total de libros por fuente, precio promedio y rating promedio.
- Histograma de precios.
- Libros por categoría.
- Distribución de ratings de una a cinco estrellas.
- Top 10 más caros.
- Top 10 mejor calificados, desempatados por precio.
- Tabla combinada con filtros por título, autor y categoría.

## Estructura

```text
.
├── dags/
│   └── pipeline_books.py   # DAG, tareas ETL y dependencias
├── streamlit/
│   └── app.py              # Dashboard de solo lectura
├── docker-compose.yml      # Infraestructura local
├── GUIA_BOOKS.md           # Chuleta rápida de operación
└── README.md               # Documentación principal
```

## Puntos importantes para exponerlo

1. **Airflow orquesta; no es la base de datos.** Define dependencias,
   reintentos y periodicidad.
2. **Kafka desacopla productores y consumidores.** Cada fuente tiene su topic
   para poder distinguir formatos y procesarlos después.
3. **La transformación ocurre antes de MongoDB.** Por ejemplo, `£51.77`
   pasa a `51.77`, `Three` pasa a `3` y la disponibilidad pasa a booleano.
4. **MongoDB conserva documentos flexibles.** Las fuentes tienen campos
   distintos, por lo que separarlas evita forzar un modelo artificial.
5. **Streamlit no modifica datos.** Solo consulta MongoDB y dibuja resultados.
6. **Los reintentos cubren fallos transitorios.** El DAG tiene dos reintentos
   con una espera de 30 segundos.

## Limitaciones y mejoras futuras

- Las fuentes son sitios públicos y pueden cambiar su HTML o limitar
  peticiones; en producción convendría añadir rate limiting, validación y
  observabilidad.
- El scraping recorre varias categorías y puede tardar unos minutos.
- El pipeline reemplaza datos en cada corrida y no conserva histórico.
- Las credenciales y secretos no están configurados porque el entorno es
  local; en producción deben gestionarse mediante variables seguras o un
  secret backend.
- Se podría añadir un esquema de validación, tests automatizados, volúmenes
  persistentes y un registro de ejecuciones.

## Licencia y uso

Proyecto educativo para practicar integración de datos, orquestación,
mensajería, persistencia y visualización. Las fuentes externas mantienen sus
propias condiciones de uso.
