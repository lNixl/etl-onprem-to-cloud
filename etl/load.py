import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Ruta del parquet limpio (ajusta si usas otro nombre)
PARQUET_PATH = "/opt/data/processed/netflix_tv_clean"

# Cargar credenciales desde .env
load_dotenv()
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_ACCESS_KEY")


def parse_duration_to_millis(value: str) -> int:
    """
    Convierte textos como '60s', '5m', '24h', '1000ms' a milisegundos (int).
    Si ya es número ('60000'), lo devuelve tal cual.
    """
    v = value.strip()

    if v.isdigit():
        return int(v)

    multipliers = {
        "ms": 1,
        "s": 1000,
        "m": 60 * 1000,
        "h": 60 * 60 * 1000,
    }

    for suffix, factor in multipliers.items():
        if v.endswith(suffix):
            num = v[: -len(suffix)]
            return int(num) * factor

    return int(v)


def load_to_s3():
    spark = (
        SparkSession.builder
        .appName("Load_To_S3")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )

    sc = spark.sparkContext
    hconf = sc._jsc.hadoopConfiguration()

    # Credenciales AWS
    hconf.set("fs.s3a.access.key", ACCESS_KEY)
    hconf.set("fs.s3a.secret.key", SECRET_KEY)
    hconf.set("fs.s3a.endpoint", "s3.amazonaws.com")

    # Parche: convertir valores tipo "60s", "5m", etc. en fs.s3a.* a milisegundos
    it = hconf.iterator()
    while it.hasNext():
        entry = it.next()
        key = entry.getKey()
        val = entry.getValue()
        if key.startswith("fs.s3a.") and any(c.isalpha() for c in val):
            try:
                millis = parse_duration_to_millis(val)
                hconf.set(key, str(millis))
            except Exception:
                # Si no se puede convertir, lo dejamos como está
                pass

    # Leer Parquet
    df = spark.read.parquet(PARQUET_PATH)

    # Escribir en S3 (ajusta ruta lógica si quieres)
    df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .csv("s3a://data-engineer-bucket-fam/salidaCSV/")

    print("Datos cargados correctamente en S3")

    spark.stop()


if __name__ == "__main__":
    load_to_s3()
