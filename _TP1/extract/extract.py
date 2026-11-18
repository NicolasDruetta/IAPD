import pandas as pd

def extract_bronze(file_path):
    # Cargo los datos históricos desde el archivo local.
    df_bronze = pd.read_csv(
        file_path,
        header=[0, 1],
        index_col=0,
        parse_dates=[0]
    )

    return df_bronze
