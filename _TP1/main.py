from pathlib import Path
from extract.extract import extract_bronze
from transform.transform import transform_silver, transform_silver_indices, transform_silver_nulos, transform_silver_ordenado
from load.load import load_bronze, load_silver, load_silver_indices, load_silver_nulos, load_silver_ordenado

def main():
    file_path = Path("data/indices_bursatiles.csv")
    file_path_abs = Path(__file__).resolve().parent / file_path

    df_bronze = extract_bronze(file_path_abs)
    load_bronze(df_bronze, file_path)

    df_silver = transform_silver(df_bronze)
    load_silver(df_silver)

    df_silver_indices = transform_silver_indices(df_silver)
    load_silver_indices(df_silver_indices)

    df_silver_ordenado = transform_silver_ordenado(df_silver_indices)
    load_silver_ordenado(df_silver_ordenado)

    df_silver_nulos = transform_silver_nulos(df_silver_ordenado)
    load_silver_nulos(df_silver_nulos)

if __name__ == "__main__":
    main()
