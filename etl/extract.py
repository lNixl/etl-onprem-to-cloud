import os
import shutil

# Carpeta donde llegan los archivos CSV originales (input bruto)
INPUT_DIR = "/opt/data/input"

# Carpeta donde guardarás una copia del archivo ya identificado (procesado)
PROCESSED_DIR = "/opt/data/processed"


def extract_latest_csv():
    """
    Busca en la carpeta INPUT_DIR el archivo CSV más reciente,
    le cambia los espacios por guiones bajos si es necesario
    y lo copia a la carpeta PROCESSED_DIR.
    """

    # Listar todos los archivos .csv dentro del directorio de entrada
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".csv")]

    # Si no hay archivos CSV, se lanza un error
    if not files:
        raise Exception("No se encontró ningún CSV en /data/input.")

    # Ordenar archivos alfabéticamente (el último será el más reciente si siguen un orden de nombre)
    files = sorted(files)

    # Seleccionar el último archivo de la lista (el más reciente)
    file_name = files[-1]

    # Ruta completa del archivo original
    original_path = os.path.join(INPUT_DIR, file_name)

    # Reemplazar espacios en el nombre por guiones bajos
    # Ej: "NYC File 2023.csv" --> "NYC_File_2023.csv"
    new_name = file_name.replace(" ", "_")
    new_path = os.path.join(INPUT_DIR, new_name)

    # Si el nombre cambió, renombramos el archivo
    if new_name != file_name:
        os.rename(original_path, new_path)

    # Definir la nueva ruta en la carpeta "processed"
    processed_path = os.path.join(PROCESSED_DIR, new_name)

    # Mensajes informativos
    print(f"[EXTRACT] Archivo encontrado: {new_name}")
    print(f"[EXTRACT] Ruta: {new_path}")

    # Copiar el archivo a la carpeta processed
    # IMPORTANTE: Spark seguirá leyendo el archivo desde /input,
    # processed solo guarda un respaldo
    shutil.copy(new_path, processed_path)

    # Retornar la ruta del archivo (puede servir en otras funciones)
    return new_path


# Este bloque solo se ejecuta cuando corres este archivo directamente
# (no cuando se importa desde otro script)
if __name__ == "__main__":
    extract_latest_csv()
