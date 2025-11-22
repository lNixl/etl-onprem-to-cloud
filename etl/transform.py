from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, when, regexp_replace, trim, udf
from pyspark.sql.types import StringType

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("Netflix_TV_Shows_Clean") \
    .getOrCreate()

# Rutas
input_path = "/opt/data/input/netflix_titles.csv"
output_path = "/opt/data/processed/netflix_tv_clean"

# 1. Leer CSV
df = spark.read.csv(input_path, header=True, inferSchema=True)

# 2. Filtrar SOLO TV Shows
df = df.filter(col("type") == "TV Show")

# 3. Crear columna main_genre
df = df.withColumn(
    "main_genre",
    split(col("listed_in"), ",")[0]
)

# ===== NUEVO: LIMPIEZA Y FILTRO POR PAÍSES DE EUROPA =====

# Lista de países europeos que aparecen (o pueden aparecer) en tu dataset
EUROPE_COUNTRIES_STD = {
    "United Kingdom": "United Kingdom",
    "England": "United Kingdom",
    "Scotland": "United Kingdom",
    "Wales": "United Kingdom",
    "Ireland": "Ireland",
    "Spain": "Spain",
    "France": "France",
    "Germany": "Germany",
    "West Germany": "Germany",
    "Italy": "Italy",
    "Portugal": "Portugal",
    "Netherlands": "Netherlands",
    "Belgium": "Belgium",
    "Sweden": "Sweden",
    "Norway": "Norway",
    "Denmark": "Denmark",
    "Finland": "Finland",
    "Poland": "Poland",
    "Czech Republic": "Czech Republic",
    "Austria": "Austria",
    "Switzerland": "Switzerland",
    "Luxembourg": "Luxembourg",
    "Russia": "Russia",
    "Ukraine": "Ukraine",
    "Belarus": "Belarus",
    "Turkey": "Turkey",
    "Greece": "Greece",
    "Cyprus": "Cyprus",
    "Iceland": "Iceland"
}

@udf(StringType())
def get_europe_country(country_str):
    if not country_str:
        return None
    # limpiar comillas raras y espacios
    cleaned = country_str.replace('"', '')
    parts = [p.strip() for p in cleaned.split(",") if p.strip() != ""]
    for p in parts:
        if p in EUROPE_COUNTRIES_STD:
            # devolver nombre estandarizado
            return EUROPE_COUNTRIES_STD[p]
    return None

# Limpiar country base (quitar comillas y espacios externos)
df = df.withColumn("country", trim(regexp_replace(col("country"), '"', '')))

# Obtener país europeo estandarizado (si existe)
df = df.withColumn("country_eu", get_europe_country(col("country")))

# Nos quedamos solo con filas que tienen algún país europeo
df = df.filter(col("country_eu").isNotNull())

# ==========================================================

# 5. LIMPIAR release_year (solo números)
df = df.withColumn(
    "release_year",
    regexp_replace(col("release_year"), "[^0-9]", "")
)

df = df.filter(col("release_year") != "")

# Convertir a entero
df = df.withColumn("release_year", col("release_year").cast("int"))

# 6. Ratings válidos
ratings_validos = [
    "TV-G", "TV-Y", "TV-Y7", "TV-Y7-FV",
    "TV-PG", "TV-14", "TV-MA",
    "G", "PG", "PG-13", "R", "NC-17", "NR"
]

# 7. Limpiar rating
df = df.withColumn(
    "rating_clean",
    when(col("rating").isin(ratings_validos), col("rating")).otherwise("UNKNOWN")
)

# 8. Crear significado del rating
df = df.withColumn(
    "rating_meaning",
    when(col("rating_clean") == "TV-G", "Todo público")
    .when(col("rating_clean") == "TV-Y", "Niños pequeños")
    .when(col("rating_clean") == "TV-Y7", "Mayores de 7 años")
    .when(col("rating_clean") == "TV-Y7-FV", "7+ con violencia fantasiosa")
    .when(col("rating_clean") == "TV-PG", "Guía parental recomendada")
    .when(col("rating_clean") == "TV-14", "Mayores de 14 años")
    .when(col("rating_clean") == "TV-MA", "Solo adultos")
    .when(col("rating_clean") == "G", "Todo público")
    .when(col("rating_clean") == "PG", "Guía parental")
    .when(col("rating_clean") == "PG-13", "Mayores de 13 años")
    .when(col("rating_clean") == "R", "Restringido +17")
    .when(col("rating_clean") == "NC-17", "Solo adultos (estricto)")
    .when(col("rating_clean") == "NR", "No clasificado")
    .otherwise("Desconocido")
)

# 9. ESTANDARIZACIÓN GENERAL DE GÉNEROS
df = df.withColumn(
    "genre_group",
    when(col("main_genre").like("%Kids%"), "Kids")
    .when(col("main_genre").like("%Anime%"), "Anime")
    .when(col("main_genre").like("%Crime%"), "Crime")
    .when(col("main_genre").like("%Horror%"), "Crime")
    .when(col("main_genre").like("%Action%"), "Crime")
    .when(col("main_genre").like("%Drama%"), "Drama")
    .when(col("main_genre").like("%Romantic%"), "Drama")
    .when(col("main_genre").like("%Classic%"), "Drama")
    .when(col("main_genre").like("%Comedy%"), "Comedy")
    .when(col("main_genre").like("%Stand-Up%"), "Comedy")
    .when(col("main_genre").like("%Reality%"), "Documental")
    .when(col("main_genre").like("%Docu%"), "Documental")
    .otherwise("Other")
)

# 10. Seleccionar columnas finales (usando el país europeo estandarizado)
df_clean = df.select(
    col("show_id"),
    col("title"),
    col("country_eu").alias("country"),
    col("release_year"),
    col("rating_clean").alias("rating"),
    col("rating_meaning"),
    col("main_genre"),
    col("genre_group")
)

# 11. Guardar en PARQUET limpio
df_clean.write.mode("overwrite").parquet(output_path)

print("Netflix TV Shows SOLO EUROPA, LIMPIO + ESTANDARIZADO:", output_path)

spark.stop()
