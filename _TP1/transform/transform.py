def transform_data(df_bronze):
    # Convierto los datos de Bronze de formato ancho a largo, sin modificar el original.
    df_silver = df_bronze.stack(level=1, future_stack=True).reset_index()

    # Quito el nombre del eje de columnas.
    df_silver.columns.name = None

    return df_silver
