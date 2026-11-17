from extract.extract import extract_data
from load.load import load_data
from pathlib import Path
from transform.transform import transform_data

def main():
    project_path = Path(__file__).resolve().parent
    file_path = project_path / "data" / "indices_bursatiles.csv"

    df_bronze = extract_data(file_path)
    df_silver = transform_data(df_bronze)
    load_data(df_bronze, df_silver, "data/indices_bursatiles.csv")

if __name__ == "__main__":
    main()
