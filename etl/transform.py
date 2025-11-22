from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, datediff
)

# Spark Session
spark = SparkSession.builder \
    .appName("NYC311_Transform") \
    .getOrCreate()

# Ruta del CSV
input_path = "/opt/data/input/NYC_311_Service_Requests.csv"
output_path = "/opt/data/processed/nyc311_clean"

# 1. Leer CSV original
df = spark.read.csv(input_path, header=True, inferSchema=True)

# 2. Convertir columnas de fecha
df = df.withColumn("created_date", to_timestamp(col("Created Date"), "MM/dd/yyyy hh:mm:ss a")) \
       .withColumn("closed_date", to_timestamp(col("Closed Date"), "MM/dd/yyyy hh:mm:ss a"))

# 3. Calcular duración del caso en días
df = df.withColumn(
    "resolution_days",
    datediff(col("closed_date"), col("created_date"))
)

# 4. Seleccionar columnas importantes
df_clean = df.select(
    col("Unique Key").alias("unique_key"),
    col("created_date"),
    col("closed_date"),
    col("Agency").alias("agency"),
    col("Complaint Type").alias("complaint_type"),
    col("Descriptor").alias("descriptor"),
    col("Incident Zip").alias("zip"),
    col("City").alias("city"),
    col("Borough").alias("borough"),
    col("Latitude").alias("latitude"),
    col("Longitude").alias("longitude"),
    col("resolution_days")
)

# 5. Limpiar nulos en campos esenciales
df_clean = df_clean.na.drop(subset=["unique_key", "created_date", "agency"])

# 6. Guardar en formato Parquet (listo para load.py)
df_clean.write.mode("overwrite").parquet(output_path)

print("Transformación completada. Archivo limpio en:", output_path)

spark.stop()
