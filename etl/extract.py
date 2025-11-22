import os
import shutil

INPUT_DIR = "/opt/data/input"
PROCESSED_DIR = "/opt/data/processed"

def extract_latest_csv():

    # Obtiene todos los archivos .csv
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".csv")]

    if not files:
        print("No hay archivos CSV")
        return

    # Toma el último archivo
    file = sorted(files)[-1]

    original_path = os.path.join(INPUT_DIR, file)
    processed_path = os.path.join(PROCESSED_DIR, file)

    # Copiar el archivo a la carpeta processed
    shutil.copy(original_path, processed_path)

    print("Archivo procesado:", file)

if __name__ == "__main__":
    extract_latest_csv()
