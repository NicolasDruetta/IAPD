def transform_silver(df_bronze):
    # Convierto los datos de Bronze de formato ancho a largo, sin modificar el original.
    df_silver = df_bronze.stack(level=1, future_stack=True).reset_index()

    # Quito el nombre del eje de columnas.
    df_silver.columns.name = None

    return df_silver

def transform_silver_indices(df_silver):
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

    return df_silver_indices

def transform_silver_ordenado(df_silver_indices):
    # Creo una copia para ordenar las columnas del dataset.
    df_silver_ordenado = df_silver_indices.copy()

    # Defino el orden de las columnas.
    columnas = ["Date", "País", "Índice", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]

    # Organizo las columnas sin modificar el DataFrame anterior.
    df_silver_ordenado = df_silver_ordenado[columnas].sort_values(["País", "Date"]).copy()

    return df_silver_ordenado

def transform_silver_cols_nulas(df_silver_ordenado):
    # Reviso cuántos valores nulos tiene cada columna.
    df_silver_cols_nulas = df_silver_ordenado.isnull().sum().reset_index()
    df_silver_cols_nulas.columns = ["Columna", "Nulos"]

    # Calculo el porcentaje de valores nulos sobre el total de registros.
    df_silver_cols_nulas["Porcentaje (%)"] = (df_silver_cols_nulas["Nulos"] / len(df_silver_ordenado) * 100).round(2)

    return df_silver_cols_nulas

def transform_silver_dias_nulos(df_silver_ordenado):
    # Creo una copia con los registros que no tienen precio de cierre.
    df_silver_dias_nulos = df_silver_ordenado[df_silver_ordenado["Close"].isna()].copy()

    # Defino los nombres de los días de lunes a viernes.
    dias = {
        0: "Lunes",
        1: "Martes",
        2: "Miércoles",
        3: "Jueves",
        4: "Viernes"
    }

    # Identifico el día de la semana de cada registro.
    df_silver_dias_nulos["Día"] = df_silver_dias_nulos["Date"].dt.day_of_week.map(dias)

    # Agrupo los registros sin cotización por país y día de la semana.
    df_silver_dias_nulos = df_silver_dias_nulos.groupby(
        ["País", "Día"]
    ).size().unstack(fill_value=0)

    # Ordeno las columnas de lunes a viernes.
    df_silver_dias_nulos = df_silver_dias_nulos.reindex(columns=list(dias.values()), fill_value=0)

    # Calculo el total de registros sin cotización por país.
    df_silver_dias_nulos["Total"] = df_silver_dias_nulos.sum(axis=1)

    return df_silver_dias_nulos

def transform_silver_sin_cotizacion(df_silver_ordenado):
    # Defino las columnas bursátiles que voy a revisar.
    columnas_precios = ["Adj Close", "Close", "High", "Low", "Open", "Volume"]

    # Defino los nombres de los días de la semana.
    dias = {
        0: "Lunes",
        1: "Martes",
        2: "Miércoles",
        3: "Jueves",
        4: "Viernes"
    }

    # Identifico los días de semana que no tienen ningún dato bursátil.
    df_silver_sin_cotizacion = df_silver_ordenado[
        df_silver_ordenado[columnas_precios].isna().all(axis=1)
    ].copy()

    # Agrego el día de la semana después de la fecha.
    df_silver_sin_cotizacion.insert(
        1,
        "Día",
        df_silver_sin_cotizacion["Date"].dt.day_of_week.map(dias)
    )

    return df_silver_sin_cotizacion

def transform_silver_incompletos(df_silver_ordenado):
    # Defino las columnas bursátiles que voy a revisar.
    columnas_precios = ["Adj Close", "Close", "High", "Low", "Open", "Volume"]

    # Identifico los días de semana que tienen algunos datos faltantes.
    df_silver_incompletos = df_silver_ordenado[
        df_silver_ordenado[columnas_precios].isna().any(axis=1)
        & ~df_silver_ordenado[columnas_precios].isna().all(axis=1)
    ].copy()

    return df_silver_incompletos

def transform_silver_depurado(df_silver_ordenado):
    # Defino las columnas bursátiles que voy a revisar.
    columnas_precios = ["Adj Close", "Close", "High", "Low", "Open", "Volume"]

    # Creo una copia sin los registros que tienen todos los datos bursátiles vacíos.
    df_silver_depurado = df_silver_ordenado.dropna(subset=columnas_precios, how="all").copy()

    return df_silver_depurado

def transform_silver_validacion_final(df_silver_depurado):
    # Compruebo si quedaron valores nulos después de la limpieza.
    nulos = df_silver_depurado.isna().sum().sum()

    # Compruebo si hay fechas duplicadas para un mismo índice bursátil.
    duplicados = df_silver_depurado.duplicated(subset=["Date", "Ticker"]).sum()

    return nulos, duplicados
