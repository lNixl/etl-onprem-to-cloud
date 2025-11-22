import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Ruta donde se encuentra el dataset en formato Parquet ya procesado.
# Normalmente es la salida de un paso anterior de tu pipeline (ETL/limpieza).
PARQUET_PATH = "/opt/data/processed/nyc311_clean"

# Carga las variables de entorno desde el archivo .env ubicado en el proyecto.
# Esto permite NO hardcodear las credenciales en el código.
load_dotenv()

# Lee las credenciales de AWS desde las variables de entorno.
# En tu archivo .env deben existir:
# ACCESS_KEY=<tu_access_key>
# SECRET_ACCESS_KEY=<tu_secret_key>
accessKeyId = os.getenv("ACCESS_KEY")
secretAccessKey = os.getenv("SECRET_ACCESS_KEY")


def parse_duration_to_millis(value: str) -> int:
    """
    Convierte strings tipo '60s', '5m', '24h', '1000ms' a milisegundos (int).
    Si ya es un número, lo devuelve tal cual.

    Ejemplos:
    - '60s'  -> 60000
    - '5m'   -> 300000
    - '2h'   -> 7200000
    - '1000ms' -> 1000
    - '60000'  -> 60000 (ya está en ms)

    Esta función se usa como "parche" para convertir configuraciones
    de Hadoop/S3 del estilo '60s' o '24h' a valores numéricos válidos
    para Spark, ya que a veces algunos conectores esperan milisegundos
    y fallan si ven sufijos de texto.
    """
    value = value.strip()

    # Si son solo dígitos (ej. '60000'), asumimos que ya está en milisegundos.
    if value.isdigit():
        return int(value)

    # Tabla de conversión de sufijos de tiempo a milisegundos.
    multipliers = {
        "ms": 1,
        "s": 1000,
        "m": 60 * 1000,
        "h": 60 * 60 * 1000,
    }

    # Recorremos cada sufijo posible y, si matchea, convertimos.
    for suffix, factor in multipliers.items():
        if value.endswith(suffix):
            # Parte numérica sin el sufijo (ej. '60' de '60s')
            num_part = value[: -len(suffix)]
            return int(num_part) * factor

    # Si nada matchea, intentamos convertirlo directamente a int
    # (por si viene en algún formato numérico no contemplado).
    return int(value)


def load_to_s3():
    """
    Crea una sesión de Spark, aplica un parche a las configuraciones S3
    que usan sufijos de tiempo, lee un Parquet local y sube una muestra
    de 10 registros a un bucket S3 usando el conector s3a.
    """
    spark = (
        SparkSession.builder
        .appName("NYC311_Load_S3")  # Nombre de la aplicación Spark para identificarla en logs/UI
        .config(
            "spark.jars.packages",
            # Dependencias necesarias para que Spark pueda hablar con S3 vía s3a:
            # - hadoop-aws: integración de Hadoop con AWS
            # - aws-java-sdk-bundle: SDK de AWS en Java (para S3, etc.)
            "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            # Proveedor de credenciales: usaremos las keys que seteamos en la config Hadoop.
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Implementación del FileSystem S3A para que Spark entienda la URI 's3a://...'
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Uso de path-style access (útil para compatibilidad con ciertos endpoints / S3 compatibles)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # Endpoint de S3. En este caso, el endpoint público general de AWS S3.
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .getOrCreate()
    )

    # Contexto de Spark para acceder a configuración de Hadoop (fs.s3a.*).
    sc = spark.sparkContext
    hconf = sc._jsc.hadoopConfiguration()

    # --- CONFIGURACIÓN DE CREDENCIALES AWS PARA S3A ---

    # Se configuran las credenciales de acceso a S3 dentro del stack Hadoop.
    # Estas son las credenciales que se usan cuando Spark lee/escribe a 's3a://...'.
    hconf.set("fs.s3a.access.key", accessKeyId)
    hconf.set("fs.s3a.secret.key", secretAccessKey)

    # --- PARCHE GENÉRICO DE DURACIONES fs.s3a.* ---

    # Algunas propiedades fs.s3a.* pueden venir con valores tipo "60s", "5m", "24h", etc.,
    # y ciertos componentes esperan valores numéricos (milisegundos). Para evitar errores,
    # se recorre toda la configuración, se detectan las claves que empiecen con 'fs.s3a.'
    # y cuyos valores tengan letras, y se intenta convertir esos valores a milisegundos.
    it = hconf.iterator()
    while it.hasNext():
        entry = it.next()
        key = entry.getKey()
        val = entry.getValue()

        # Filtramos solo propiedades relacionadas a s3a que contienen letras (s, m, h, etc.)
        if key.startswith("fs.s3a.") and any(c.isalpha() for c in val):
            try:
                # Intentamos convertir el valor a milisegundos usando la función auxiliar.
                millis = parse_duration_to_millis(val)
                hconf.set(key, str(millis))
                print(f"[PATCH] {key} = {val}  ->  {millis}")
            except Exception as e:
                # Si por alguna razón no se puede convertir, solo lo reportamos
                # pero no detenemos la ejecución.
                print(f"[WARN] No se pudo convertir {key}={val}: {e}")

    # --- LECTURA DEL PARQUET LOCAL ---

    # Leemos el dataset en formato Parquet desde la ruta local definida.
    # Spark inferirá el esquema y cargará el DataFrame completo.
    df = spark.read.parquet(PARQUET_PATH)

    # Para pruebas, tomamos solo 10 registros.
    # Esto permite validar la escritura en S3 sin subir todo el dataset.
    df_sample = df.limit(10)
    print(f"[LOAD] Registros a cargar (sample): {df_sample.count()}")

    # --- ESCRITURA EN S3 ---

    # Escribimos el DataFrame de ejemplo (10 registros) en un bucket de S3.
    # - mode("overwrite"): si ya existe algo en esa ruta, lo sobreescribe.
    # - option("compression", "snappy"): utiliza compresión Snappy (muy común con Parquet).
    # El path de salida es un prefijo dentro del bucket:
    # s3a://data-engineer-bucket-fam/nyc311/clean/
    df_sample.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet("s3a://data-engineer-bucket-fam/nyc311/clean/")

    print("[LOAD] Sample cargado correctamente en S3.")

    # Cerramos la sesión de Spark para liberar recursos.
    spark.stop()


if __name__ == "__main__":
    # Punto de entrada del script.
    # Si se ejecuta este archivo directamente con `python archivo.py`,
    # se ejecutará la función load_to_s3().
    load_to_s3()
