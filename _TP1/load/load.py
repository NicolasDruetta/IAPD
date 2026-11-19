import time

from app.functions import configure_pandas, print_title

def run_gold(df_silver):
    configure_pandas()
    df_gold = load_gold(df_silver)
    time.sleep(0.3)

    df_cierres_anuales = load_gold_cierres_anuales(df_gold)

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

    print_title("[Gold] Rendimiento anual de los índices bursátiles:")
    print(df_cierres_anuales.sort_values(["País", "Año"]).to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_cierres_anuales
