from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, when, regexp_replace, trim,
    udf, concat, lit
)
from pyspark.sql.types import StringType

# =========================
# 1. Crear sesión Spark
# =========================
spark = SparkSession.builder \
    .appName("Netflix_TV_Shows_Clean") \
    .getOrCreate()

# Rutas
input_path = "/opt/data/input/netflix_titles.csv"
output_path = "/opt/data/processed/netflix_tv_clean"

# =========================
# 2. Leer CSV original
# =========================
df = spark.read.csv(input_path, header=True, inferSchema=True)

# =========================
# 3. Filtrar SOLO TV Shows
# =========================
df = df.filter(col("type") == "TV Show")

# =========================
# 4. Columna main_genre
# =========================
df = df.withColumn(
    "main_genre",
    split(col("listed_in"), ",")[0]
)

# ==========================================================
# 5. LIMPIEZA Y FILTRO POR PAÍSES DE EUROPA
# ==========================================================

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
    # limpiar comillas y espacios
    cleaned = country_str.replace('"', '')
    parts = [p.strip() for p in cleaned.split(",") if p.strip() != ""]
    for p in parts:
        if p in EUROPE_COUNTRIES_STD:
            return EUROPE_COUNTRIES_STD[p]
    return None

# Limpiar base de country (quitar comillas y espacios externos)
df = df.withColumn("country", trim(regexp_replace(col("country"), '"', '')))

# Obtener país europeo estandarizado
df = df.withColumn("country_eu", get_europe_country(col("country")))

# Quedarnos solo con filas que tienen país europeo
df = df.filter(col("country_eu").isNotNull())

# ==========================================================
# 6. LIMPIAR release_year (solo números)
# ==========================================================
df = df.withColumn(
    "release_year",
    regexp_replace(col("release_year"), "[^0-9]", "")
)

df = df.filter(col("release_year") != "")
df = df.withColumn("release_year", col("release_year").cast("int"))

# ==========================================================
# 7. Normalizar rating y significado
# ==========================================================
ratings_validos = [
    "TV-G", "TV-Y", "TV-Y7", "TV-Y7-FV",
    "TV-PG", "TV-14", "TV-MA",
    "G", "PG", "PG-13", "R", "NC-17", "NR"
]

# rating limpio
df = df.withColumn(
    "rating_clean",
    when(col("rating").isin(ratings_validos), col("rating")).otherwise("UNKNOWN")
)

# significado de rating
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

# como capa extra de protección, si por alguna razón rating_clean no es válido:
df = df.withColumn(
    "rating_meaning",
    when(col("rating_clean").isin(ratings_validos), col("rating_meaning"))
    .otherwise("Desconocido")
)

# ==========================================================
# 8. Agrupar géneros en genre_group
# ==========================================================
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

# ==========================================================
# 9. Limpieza extra de textos (comillas + comas en title)
# ==========================================================

# Quitar comillas dobles de columnas de texto
cols_to_clean = ["title", "country", "country_eu", "main_genre", "rating_meaning"]
for c in cols_to_clean:
    df = df.withColumn(c, regexp_replace(col(c), '"', ''))

# Reemplazar comas en el título por " - "
# Esto evita que, al exportar a CSV, las comas rompan las columnas
df = df.withColumn("title", regexp_replace(col("title"), ",", " -"))

# Eliminar espacios extremos
df = df.withColumn("title", trim(col("title")))
df = df.withColumn("rating_meaning", trim(col("rating_meaning")))
df = df.withColumn("main_genre", trim(col("main_genre")))
df = df.withColumn("country_eu", trim(col("country_eu")))

# ==========================================================
# 10. Seleccionar columnas finales
# ==========================================================
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

# (Opcional) Por si acaso, filtrar cualquier basura rara en rating_meaning
df_clean = df_clean.filter(~col("rating_meaning").isin("TV-MA", "TV-G", "2020"))

# ==========================================================
# 11. Guardar en PARQUET limpio
# ==========================================================
df_clean.write.mode("overwrite").parquet(output_path)

print("Netflix TV Shows SOLO EUROPA, LIMPIO + ESTANDARIZADO:", output_path)

spark.stop()
