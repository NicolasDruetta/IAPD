import time

from app.functions import NOMBRES_MESES, configure_pandas, print_title

def run_gold(df_silver):
    configure_pandas()

    df_gold = load_gold(df_silver)
    time.sleep(0.3)

    df_cierres_anuales = load_gold_cierres_anuales(df_gold)
    time.sleep(0.3)

    load_gold_periodo_analisis()
    time.sleep(0.3)

    df_rendimientos_anuales = load_gold_rendimientos_anuales(df_cierres_anuales)
    time.sleep(0.3)

    df_rendimiento_acumulado = load_gold_rendimiento_acumulado(df_cierres_anuales)
    time.sleep(0.3)

    df_correlaciones = load_gold_correlaciones(df_gold)
    time.sleep(0.3)

    df_resumen_mercados = load_gold_mercados(df_rendimientos_anuales)
    time.sleep(0.3)

    df_tendencia_reciente = load_gold_tendencia(df_rendimientos_anuales)
    time.sleep(0.3)

    df_volumen_relativo = load_gold_volumen_mensual(df_gold)
    time.sleep(0.3)

    df_resumen_volumen = load_gold_volumen_promedio(df_volumen_relativo)
    time.sleep(0.3)

    df_resumen_frecuencia_volumen = load_gold_frecuencia_volumen(df_volumen_relativo)
    time.sleep(0.3)

    df_cobertura_volumen = load_gold_cobertura_volumen(df_gold)

    return df_gold

def load_gold(df_silver):
    # Creo una copia de los datos limpios para trabajar en Gold.
    df_gold = df_silver.copy()

    # Ordeno los registros por país y fecha.
    df_gold = df_gold.sort_values(["País", "Date"]).reset_index(drop=True).copy()

    # Calculo el rendimiento diario de cada índice bursátil.
    df_gold["Rendim. diario (%)"] = (
        df_gold.groupby("Ticker")["Close"].pct_change(fill_method=None) * 100
    ).round(2)

    print_title("[Gold] Datos preparados:")
    print(df_gold.to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_gold

def load_gold_cierres_anuales(df_gold):
    # Calculo el último precio de cierre disponible de cada índice por año.
    df_cierres_anuales = df_gold.groupby(
        ["País", "Índice", "Ticker", "Año"], as_index=False
    ).agg({
        "Close": "last"
    })

    # Calculo el rendimiento anual de cada índice bursátil.
    df_cierres_anuales["Rendim. anual (%)"] = (
        df_cierres_anuales.groupby("Ticker")["Close"].pct_change(fill_method=None) * 100
    ).round(2)

    print_title("[Gold] Rendimiento anual de los índices bursátiles (2006-2026):")
    print(df_cierres_anuales.sort_values(["País", "Año"]).to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_cierres_anuales

def load_gold_periodo_analisis():
    print()
    print("* A partir de ahora el período de análisis es: 2007-2025.")
    print("** Se excluyen 2006 y 2026 porque son años incompletos en el dataset.")
    print("*** Se utilizan únicamente años completos para garantizar comparaciones anuales consistentes.")
    print(flush=True)

def load_gold_rendimientos_anuales(df_cierres_anuales):
    # Creo una copia con los rendimientos de los años completos.
    df_rendimientos_anuales = df_cierres_anuales[
        df_cierres_anuales["Año"].between(2007, 2025)
    ].copy()

    # Ordeno los resultados por país y año.
    df_rendimientos_anuales = df_rendimientos_anuales.sort_values(
        ["País", "Año"]
    ).reset_index(drop=True)

    # Muestro los rendimientos anuales del período completo.
    print_title("[Gold] Rendimientos anuales de los índices bursátiles (2007-2025):")
    print(df_rendimientos_anuales.to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_rendimientos_anuales

def load_gold_rendimiento_acumulado(df_cierres_anuales):
    df_cierres_periodo = df_cierres_anuales[
        df_cierres_anuales["Año"].isin([2006, 2025])
    ].copy()

    df_precios_periodo = df_cierres_periodo.pivot(
        index=["País", "Índice", "Ticker"],
        columns="Año",
        values="Close"
    ).reset_index()
    df_precios_periodo = df_precios_periodo.rename(
        columns={2006: "Precio inicial", 2025: "Precio final"}
    )

    df_rendimiento_acumulado = df_precios_periodo.copy()
    df_rendimiento_acumulado["Rendim. acumulado (%)"] = (
        (df_rendimiento_acumulado["Precio final"] / df_rendimiento_acumulado["Precio inicial"] - 1) * 100
    ).round(2)

    print_title("[Gold] Rendimiento acumulado de los índices bursátiles (2007-2025):")
    print(df_rendimiento_acumulado.sort_values("País").to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_rendimiento_acumulado

def load_gold_correlaciones(df_gold):
    df_gold_periodo = df_gold[
        df_gold["Año"].between(2007, 2025)
    ]

    df_rendimientos_paises = df_gold_periodo.pivot(
        index="Date",
        columns="País",
        values="Rendim. diario (%)"
    )
    df_correlaciones = df_rendimientos_paises.corr().round(2)

    print_title("[Gold] Correlación entre los índices bursátiles (2007-2025):")
    print(df_correlaciones.to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_correlaciones

def load_gold_mercados(df_rendimientos_anuales):
    mercados_desarrollados = [
        "Alemania", "Australia", "Canadá", "España", "Estados Unidos",
        "Francia", "Hong Kong", "Japón", "Nueva Zelanda", "Países Bajos",
        "Reino Unido", "Suiza"
    ]
    mercados_emergentes = [
        "Brasil", "China", "Corea del Sur", "India", "México", "Taiwán"
    ]
    clasificacion_mercados = {
        pais: "Desarrollado" for pais in mercados_desarrollados
    }
    clasificacion_mercados.update(
        {pais: "Emergente" for pais in mercados_emergentes}
    )

    df_gold_mercados = df_rendimientos_anuales.copy()
    df_gold_mercados["Mercado"] = df_gold_mercados["País"].map(clasificacion_mercados)

    df_metricas_mercados = df_gold_mercados.groupby(
        ["País", "Índice", "Mercado"], as_index=False
    ).agg(
        rendimiento_anual=("Rendim. anual (%)", "mean"),
        volatilidad_anual=("Rendim. anual (%)", "std")
    ).rename(
        columns={
            "rendimiento_anual": "Rendimiento anual promedio (%)",
            "volatilidad_anual": "Volatilidad anual promedio (%)"
        }
    )

    df_resumen_mercados = df_metricas_mercados.groupby(
        "Mercado", as_index=False
    )[["Rendimiento anual promedio (%)", "Volatilidad anual promedio (%)"]].mean()
    df_resumen_mercados[[
        "Rendimiento anual promedio (%)", "Volatilidad anual promedio (%)"
    ]] = df_resumen_mercados[[
        "Rendimiento anual promedio (%)", "Volatilidad anual promedio (%)"
    ]].round(2)

    print_title("[Gold] Indicadores de mercados desarrollados y emergentes (2007-2025):")
    print(df_resumen_mercados.to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_resumen_mercados

def load_gold_tendencia(df_rendimientos_anuales):
    df_gold_tendencia = df_rendimientos_anuales.copy()
    df_gold_tendencia["Peso"] = df_gold_tendencia["Año"] - 2006

    df_tendencia_reciente = df_gold_tendencia.groupby(
        ["País", "Índice", "Ticker"]
    ).apply(
        lambda grupo: (
            (grupo["Rendim. anual (%)"] * grupo["Peso"]).sum() /
            grupo["Peso"].sum()
        ),
        include_groups=False
    ).reset_index(name="Rendimiento ponderado (%)")
    df_tendencia_reciente["Rendimiento ponderado (%)"] = df_tendencia_reciente[
        "Rendimiento ponderado (%)"
    ].round(2)

    print_title("[Gold] Rendimiento anual ponderado de los índices bursátiles (2007-2025):")
    print(df_tendencia_reciente.sort_values("País").to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_tendencia_reciente

def load_gold_volumen_mensual(df_gold):
    df_gold_volumen = df_gold[
        (df_gold["País"] != "Australia")
        & df_gold["Año"].between(2007, 2025)
        & (df_gold["Volume"] > 0)
    ].copy()

    df_volumen_mensual = df_gold_volumen.groupby(
        ["País", "Índice", "Año", "Mes"], as_index=False
    )["Volume"].mean()

    df_volumen_anual = df_gold_volumen.groupby(
        ["País", "Año"], as_index=False
    )["Volume"].mean().rename(
        columns={"Volume": "Volumen anual promedio"}
    )

    df_volumen_relativo = df_volumen_mensual.merge(
        df_volumen_anual,
        on=["País", "Año"]
    )
    df_volumen_relativo["Variación del volumen (%)"] = (
        (df_volumen_relativo["Volume"] / df_volumen_relativo["Volumen anual promedio"] - 1) * 100
    ).round(2)

    print_title("[Gold] Variación mensual del volumen bursátil (2007-2025):")
    print(df_volumen_relativo.sort_values(
        ["País", "Año", "Mes"]
    ).to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_volumen_relativo

def load_gold_volumen_promedio(df_volumen_relativo):
    df_promedio_volumen_paises = df_volumen_relativo.groupby(
        ["País", "Mes"], as_index=False
    )["Variación del volumen (%)"].mean()
    df_resumen_volumen = df_promedio_volumen_paises.groupby(
        "Mes", as_index=False
    )["Variación del volumen (%)"].mean()

    df_resumen_volumen["Mes"] = df_resumen_volumen["Mes"].map(NOMBRES_MESES)
    df_resumen_volumen["Variación del volumen (%)"] = df_resumen_volumen[
        "Variación del volumen (%)"
    ].round(2)

    print_title("[Gold] Variación mensual promedio del volumen bursátil (2007-2025):")
    print(df_resumen_volumen.to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_resumen_volumen

def load_gold_frecuencia_volumen(df_volumen_relativo):
    df_frecuencia_volumen = df_volumen_relativo.copy()
    df_frecuencia_volumen["Volumen inferior"] = (
        df_frecuencia_volumen["Variación del volumen (%)"] < 0
    )

    df_frecuencia_volumen_paises = df_frecuencia_volumen.groupby(
        ["País", "Mes"], as_index=False
    )["Volumen inferior"].mean()
    df_resumen_frecuencia_volumen = df_frecuencia_volumen_paises.groupby(
        "Mes", as_index=False
    )["Volumen inferior"].mean()
    df_resumen_frecuencia_volumen["Frecuencia de menor actividad (%)"] = (
        df_resumen_frecuencia_volumen["Volumen inferior"] * 100
    ).round(2)

    df_resumen_frecuencia_volumen["Mes"] = df_resumen_frecuencia_volumen["Mes"].map(NOMBRES_MESES)
    df_resumen_frecuencia_volumen = df_resumen_frecuencia_volumen[
        ["Mes", "Frecuencia de menor actividad (%)"]
    ]

    print_title("[Gold] Frecuencia mensual de menor actividad bursátil (2007-2025):")
    print(df_resumen_frecuencia_volumen.to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_resumen_frecuencia_volumen

def load_gold_cobertura_volumen(df_gold):
    df_gold_periodo = df_gold[
        df_gold["Año"].between(2007, 2025)
    ]
    df_cobertura_volumen = df_gold_periodo.groupby(
        ["País", "Índice"], as_index=False
    ).agg(
        Registros=("Volume", "size"),
        Volumen_positivo=("Volume", lambda volumen: (volumen > 0).sum())
    )
    df_cobertura_volumen["Cobertura de volumen (%)"] = (
        df_cobertura_volumen["Volumen_positivo"] /
        df_cobertura_volumen["Registros"] * 100
    ).round(2)
    df_cobertura_volumen = df_cobertura_volumen.rename(
        columns={"Volumen_positivo": "Registros con volumen"}
    )[
        [
            "País",
            "Índice",
            "Registros",
            "Registros con volumen",
            "Cobertura de volumen (%)"
        ]
    ]

    print_title("[Gold] Cobertura del volumen bursátil (2007-2025):")
    print(df_cobertura_volumen.sort_values("País").to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_cobertura_volumen
