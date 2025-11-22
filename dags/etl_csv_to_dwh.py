# Importamos la clase DAG para definir flujos de trabajo en Airflow
from airflow import DAG
# Importamos el operador que permite ejecutar comandos Bash (en este caso, scripts Python)
from airflow.operators.bash import BashOperator
# Importamos utilidades de fecha y tiempo
from datetime import datetime, timedelta

# Argumentos por defecto que se aplican a todas las tareas del DAG
default_args = {
    "owner": "airflow",              # Propietario lógico del DAG (solo informativo)
    "depends_on_past": False,        # La ejecución de hoy NO depende del estado de la ejecución anterior
    "email_on_failure": False,       # No enviar correos cuando una tarea falle
    "retries": 1,                    # Reintentar 1 vez si la tarea falla
    "retry_delay": timedelta(minutes=1),  # Esperar 1 minuto antes de reintentar
}

# Definición del DAG usando un context manager (with)
with DAG(
    dag_id="etl_csv_to_dwh",  # Identificador único del DAG en Airflow
    default_args=default_args,  # Se aplican los argumentos por defecto definidos arriba
    description="ETL: CSV -> PySpark transform -> MySQL DWH",  # Descripción del flujo
    schedule_interval="*/5 * * * *",  # Cron: ejecutar cada 5 minutos
    start_date=datetime(2025, 1, 1),  # Fecha a partir de la cual Airflow puede empezar a programar este DAG
    catchup=False,                    # No ejecutar en bloque todos los runs atrasados desde start_date
    max_active_runs=1,                # Solo una ejecución activa del DAG al mismo tiempo
) as dag:

    # Tarea 1: EXTRACT
    # Ejecuta un script Python que extrae datos desde la fuente y genera el/los CSV
    extract = BashOperator(
        task_id="extract_csv",                    # Nombre de la tarea dentro del DAG
        bash_command="python /opt/etl/extract.py" # Comando Bash que se ejecutará
    )

    # Tarea 2: TRANSFORM
    # Ejecuta un script Python que usa PySpark para transformar/limpiar los datos
    transform = BashOperator(
        task_id="transform_with_spark",           # Nombre de la tarea
        bash_command="python /opt/etl/transform.py"  # Comando Bash que corre el script de transformación
    )

    # Tarea 3: LOAD
    # Ejecuta un script Python que carga los datos transformados hacia MySQL (Data Warehouse)
    load = BashOperator(
        task_id="load_to_mysql",                  # Nombre de la tarea
        bash_command="python /opt/etl/load.py"    # Comando Bash que corre el script de carga
    )

    # Definimos la secuencia de ejecución:
    # Primero extract, luego transform, y finalmente load.
    extract >> transform >> load
