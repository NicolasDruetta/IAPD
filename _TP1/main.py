from pathlib import Path
from extract.extract import extract_data
from transform.transform import transform_data
from load.load import load_data

def main():
    project_path = Path(__file__).resolve().parent
    file_path = project_path / "data" / "indices_bursatiles.csv"

    df_bronze = extract_data(file_path)
    df_silver, df_silver_indices, df_silver_ordenado, df_silver_nulos = transform_data(df_bronze)
    load_data(
        df_bronze,
        df_silver,
        df_silver_indices,
        df_silver_ordenado,
        df_silver_nulos,
        "data/indices_bursatiles.csv"
    )

if __name__ == "__main__":
    main()
