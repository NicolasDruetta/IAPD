import time

from app.functions import NOMBRES_DIAS, configure_pandas, print_title

def run_silver(df_bronze):
    configure_pandas()

    df_silver = transform_silver(df_bronze)
    time.sleep(0.3)

    df_silver_indices = transform_silver_indices(df_silver)
    time.sleep(0.3)

    df_silver_ordenado = transform_silver_ordenado(df_silver_indices)
    time.sleep(0.3)

    df_silver_cols_nulas = transform_silver_cols_nulas(df_silver_ordenado)
    time.sleep(0.3)

    df_silver_dias_nulos = transform_silver_dias_nulos(df_silver_ordenado)
    time.sleep(0.3)

    df_silver_sin_cotizacion = transform_silver_sin_cotizacion(df_silver_ordenado)
    df_silver_incompletos = transform_silver_incompletos(df_silver_ordenado)
    transform_silver_validacion(df_silver_sin_cotizacion, df_silver_incompletos)
    time.sleep(0.3)

    df_silver_depurado = transform_silver_depurado(df_silver_ordenado)
    time.sleep(0.3)

    transform_silver_validacion_final(df_silver_depurado)
    time.sleep(0.3)

    df_silver_fechas = transform_silver_fechas(df_silver_depurado)
    time.sleep(0.3)

    df_silver_cobertura_anual = transform_silver_cobertura_anual(df_silver_fechas)

    return df_silver_fechas

def transform_silver(df_bronze):
    # Convierto los datos de Bronze de formato ancho a largo, sin modificar el original.
    df_silver = df_bronze.stack(level=1, future_stack=True).reset_index()

    # Quito el nombre del eje de columnas.
    df_silver.columns.name = None

    print_title("[Silver] Datos en formato largo:")
    print(f"Filas: {df_silver.shape[0]}.")
    print(f"Columnas: {df_silver.shape[1]}.")
    print(f"Formato largo: {df_silver.shape[1]} columnas en Silver.")

    print_title("[Silver] Vista previa en formato largo:")
    print(df_silver.to_string(max_rows=10, min_rows=10))
    print(flush=True)

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

    print_title("[Silver] Datos con país e índice:")
    print(f"Filas: {df_silver_indices.shape[0]}.")
    print(f"Columnas: {df_silver_indices.shape[1]}.")

    print_title("[Silver] Vista previa con país e índice:")
    print(df_silver_indices.to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_silver_indices

def transform_silver_ordenado(df_silver_indices):
    # Creo una copia para ordenar las columnas del dataset.
    df_silver_ordenado = df_silver_indices.copy()

    # Defino el orden de las columnas.
    columnas = ["Date", "País", "Índice", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]

    # Organizo las columnas sin modificar el DataFrame anterior.
    df_silver_ordenado = df_silver_ordenado[columnas].sort_values(["País", "Date"]).copy()

    print_title("[Silver] Datos ordenados por país y fecha:")
    print(f"Filas: {df_silver_ordenado.shape[0]}.")
    print(f"Columnas: {df_silver_ordenado.shape[1]}.")

    print_title("[Silver] Vista previa ordenada por país y fecha:")
    print(df_silver_ordenado.to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_silver_ordenado

def transform_silver_cols_nulas(df_silver_ordenado):
    # Reviso cuántos valores nulos tiene cada columna.
    df_silver_cols_nulas = df_silver_ordenado.isnull().sum().reset_index()
    df_silver_cols_nulas.columns = ["Columna", "Nulos"]

    # Calculo el porcentaje de valores nulos sobre el total de registros.
    df_silver_cols_nulas["Porcentaje (%)"] = (df_silver_cols_nulas["Nulos"] / len(df_silver_ordenado) * 100).round(2)

    print_title("[Silver] Análisis de valores nulos:")
    print(f"Filas: {df_silver_cols_nulas.shape[0]}.")
    print(f"Columnas: {df_silver_cols_nulas.shape[1]}.")

    print_title("[Silver] Vista previa de valores nulos:")
    print(df_silver_cols_nulas.to_string(index=False))
    print(flush=True)

    return df_silver_cols_nulas

def transform_silver_dias_nulos(df_silver_ordenado):
    # Creo una copia con los registros que no tienen precio de cierre.
    df_silver_dias_nulos = df_silver_ordenado[df_silver_ordenado["Close"].isna()].copy()

    # Defino los nombres de los días de lunes a viernes.
    # Identifico el día de la semana de cada registro.
    df_silver_dias_nulos["Día"] = df_silver_dias_nulos["Date"].dt.day_of_week.map(NOMBRES_DIAS)

    # Agrupo los registros sin cotización por país y día de la semana.
    df_silver_dias_nulos = df_silver_dias_nulos.groupby(
        ["País", "Día"]
    ).size().unstack(fill_value=0)

    # Ordeno las columnas de lunes a viernes.
    df_silver_dias_nulos = df_silver_dias_nulos.reindex(
        columns=[NOMBRES_DIAS[dia] for dia in range(5)], fill_value=0
    )

    # Calculo el total de registros sin cotización por país.
    df_silver_dias_nulos["Total"] = df_silver_dias_nulos.sum(axis=1)

    print_title("[Silver] Registros sin cotización por país y día de semana:")
    print(df_silver_dias_nulos.sort_index().to_string())
    print(flush=True)

    return df_silver_dias_nulos

def transform_silver_sin_cotizacion(df_silver_ordenado):
    # Defino las columnas bursátiles que voy a revisar.
    columnas_precios = ["Adj Close", "Close", "High", "Low", "Open", "Volume"]

    # Defino los nombres de los días de la semana.
    # Identifico los días de semana que no tienen ningún dato bursátil.
    df_silver_sin_cotizacion = df_silver_ordenado[
        df_silver_ordenado[columnas_precios].isna().all(axis=1)
    ].copy()

    # Agrego el día de la semana después de la fecha.
    df_silver_sin_cotizacion.insert(
        1,
        "Día",
        df_silver_sin_cotizacion["Date"].dt.day_of_week.map(NOMBRES_DIAS)
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

def transform_silver_validacion(df_silver_sin_cotizacion, df_silver_incompletos):
    # Valido la cantidad de registros sin cotización y con datos incompletos.
    print_title("[Silver] Validación de datos bursátiles:")
    print(f"Registros con todos los datos vacíos: {len(df_silver_sin_cotizacion)}")
    print(f"Registros con algunos datos faltantes: {len(df_silver_incompletos)}")

    print_title("[Silver] Registros con todos los datos vacíos:")
    print(df_silver_sin_cotizacion.to_string(max_rows=10, min_rows=10))
    print(flush=True)

def transform_silver_depurado(df_silver_ordenado):
    # Defino las columnas bursátiles que voy a revisar.
    columnas_precios = ["Adj Close", "Close", "High", "Low", "Open", "Volume"]

    # Creo una copia sin los registros que tienen todos los datos bursátiles vacíos.
    df_silver_depurado = df_silver_ordenado.dropna(subset=columnas_precios, how="all").copy()

    print_title("[Silver] Depuración de registros vacíos:")
    print(f"Registros originales: {len(df_silver_ordenado)}")
    print(f"Registros eliminados: {len(df_silver_ordenado) - len(df_silver_depurado)}")
    print(f"Registros conservados: {len(df_silver_depurado)}")

    print_title("[Silver] Datos depurados:")
    print(df_silver_depurado.to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_silver_depurado

def transform_silver_validacion_final(df_silver_depurado):
    # Compruebo si quedaron valores nulos después de la limpieza.
    nulos_restantes = df_silver_depurado.isna().sum().sum()

    # Compruebo si hay fechas duplicadas para un mismo índice bursátil.
    fechas_duplicadas = df_silver_depurado.duplicated(subset=["Date", "Ticker"]).sum()

    print_title("[Silver] Validación final de los datos depurados:")
    print(f"Registros conservados: {len(df_silver_depurado)}")
    print(f"Registros con valores nulos: {nulos_restantes}")
    print(f"Fechas duplicadas por índice: {fechas_duplicadas}")
    print(flush=True)

    # Defino las columnas de precios que voy a validar.
    columnas_precios = ["Adj Close", "Close", "High", "Low", "Open"]

    # Compruebo que los precios sean positivos y que el volumen no sea negativo.
    precios_invalidos = (df_silver_depurado[columnas_precios] <= 0).any(axis=1).sum()
    volumen_invalido = (df_silver_depurado["Volume"] < 0).sum()

    # Compruebo que los precios máximos y mínimos sean coherentes.
    maximos_invalidos = (df_silver_depurado["High"] < df_silver_depurado[["Close", "Low", "Open"]].max(axis=1)).sum()
    minimos_invalidos = (df_silver_depurado["Low"] > df_silver_depurado[["Close", "High", "Open"]].min(axis=1)).sum()

    print_title("[Silver] Validación de precios y volumen:")
    print(f"Registros con precios no válidos: {precios_invalidos}")
    print(f"Registros con volumen negativo: {volumen_invalido}")
    print(f"Registros con máximos inconsistentes: {maximos_invalidos}")
    print(f"Registros con mínimos inconsistentes: {minimos_invalidos}")
    print(flush=True)

    # Compruebo el formato de las fechas y el período disponible.

    print_title("[Silver] Validación del formato de fechas:")
    print(f"Tipo de dato: {df_silver_depurado['Date'].dtype}")
    print(f"Fechas no válidas: {df_silver_depurado['Date'].isna().sum()}")
    print(f"Fecha mínima: {df_silver_depurado['Date'].min().date()}")
    print(f"Fecha máxima: {df_silver_depurado['Date'].max().date()}")
    print(flush=True)

def transform_silver_fechas(df_silver_depurado):
    # Creo una copia para incorporar las variables temporales.
    df_silver_fechas = df_silver_depurado.copy()

    # Agrego las columnas de año y mes a partir de la fecha.
    df_silver_fechas["Año"] = df_silver_fechas["Date"].dt.year
    df_silver_fechas["Mes"] = df_silver_fechas["Date"].dt.month

    # Muestro los datos con las nuevas columnas.

    print_title("[Silver] Variables temporales:")
    print(df_silver_fechas.to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_silver_fechas

def transform_silver_cobertura_anual(df_silver_fechas):
    # Calculo cuántas cotizaciones tengo por país, índice y año.
    df_silver_cobertura_anual = df_silver_fechas.groupby(
        ["País", "Índice", "Año"]
    ).size().reset_index(name="Registros")

    # Muestro la cobertura anual ordenada por país y año.

    print_title("[Silver] Cobertura anual de los índices bursátiles:")
    print(df_silver_cobertura_anual.sort_values(["País", "Año"]).to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_silver_cobertura_anual
