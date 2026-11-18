import pandas as pd
import time

from pathlib import Path

def run_bronze():
    file_path = Path(__file__).resolve().parent.parent / "data/indices_bursatiles.csv"
    df_bronze = extract_bronze(file_path)
    time.sleep(0.3)
    return df_bronze

def extract_bronze(file_path):
    # Cargo los datos históricos desde el archivo local.
    df_bronze = pd.read_csv(
        file_path,
        header=[0, 1],
        index_col=0,
        parse_dates=[0]
    )

    print(flush=True) # Aseguro que se impriman los mensajes antes de cargar el archivo.
    print("--------------------------------")
    print("[Bronze] Datos en formato ancho:")
    print("--------------------------------")
    print(f"Archivo cargado: {file_path.relative_to(Path(__file__).resolve().parent.parent)}.")
    print(f"Filas: {df_bronze.shape[0]}.")
    print(f"Columnas: {df_bronze.shape[1]}.")
    print(f"Formato ancho: {df_bronze.shape[1]} columnas en Bronze.")
    print()
    print("---------------------------------------")
    print("[Bronze] Vista previa en formato ancho:")
    print("---------------------------------------")
    print(df_bronze.to_string(max_rows=10, min_rows=10, max_cols=6))
    print(flush=True)

    return df_bronze
