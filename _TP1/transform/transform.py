def transform_data(df_bronze):
    # Convierto los datos de Bronze de formato ancho a largo, sin modificar el original.
    df_silver = df_bronze.stack(level=1, future_stack=True).reset_index()

    # Quito el nombre del eje de columnas.
    df_silver.columns.name = None

    indices = {
        "Alemania": ("DAX 40", "^GDAXI"),
        "Australia": ("S&P/ASX 200", "^AXJO"),
        "Brasil": ("Ibovespa", "^BVSP"),
        "Canadá": ("S&P/TSX Composite", "^GSPTSE"),
        "China": ("Shanghai Composite", "000001.SS"),
        "Corea del Sur": ("KOSPI Composite", "^KS11"),
        "España": ("IBEX 35", "^IBEX"),
        "Estados Unidos": ("S&P 500", "^GSPC"),
        "Francia": ("CAC 40", "^FCHI"),
        "Hong Kong": ("Hang Seng", "^HSI"),
        "India": ("BSE Sensex", "^BSESN"),
        "Japón": ("Nikkei 225", "^N225"),
        "México": ("S&P/BMV IPC", "^MXX"),
        "Nueva Zelanda": ("S&P/NZX 50", "^NZ50"),
        "Países Bajos": ("AEX", "^AEX"),
        "Reino Unido": ("FTSE 100", "^FTSE"),
        "Suiza": ("SMI", "^SSMI"),
        "Taiwán": ("TAIEX", "^TWII")
    }

    # Creo una copia para incorporar el país y el nombre de cada índice bursátil.
    df_silver_indices = df_silver.copy()

    # Relaciono cada ticker con su país y el nombre del índice.
    datos_indices = {
        ticker: (pais, nombre)
        for pais, (nombre, ticker) in indices.items()
    }

    # Agrego las columnas de país e índice.
    df_silver_indices["País"] = df_silver_indices["Ticker"].map(lambda ticker: datos_indices[ticker][0])
    df_silver_indices["Índice"] = df_silver_indices["Ticker"].map(lambda ticker: datos_indices[ticker][1])

    # Creo una copia para ordenar las columnas del dataset.
    df_silver_ordenado = df_silver_indices.copy()

    # Defino el orden de las columnas.
    columnas = ["Date", "País", "Índice", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]

    # Organizo las columnas sin modificar el DataFrame anterior.
    df_silver_ordenado = df_silver_ordenado[columnas].sort_values(["País", "Date"]).copy()

    return df_silver, df_silver_indices, df_silver_ordenado
