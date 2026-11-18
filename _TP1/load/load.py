from app.functions import print_title

def run_gold(df_silver):
    df_gold = load_gold(df_silver)
    return df_gold

def load_gold(df_silver):
    # Creo una copia de los datos limpios para trabajar en Gold.
    df_gold = df_silver.copy()

    # Ordeno los registros por país y fecha.
    df_gold = df_gold.sort_values(["País", "Date"]).reset_index(drop=True).copy()

    # Calculo el rendimiento diario de cada índice bursátil.
    df_gold["Rendimiento diario (%)"] = (
        df_gold.groupby("Ticker")["Close"].pct_change(fill_method=None) * 100
    ).round(2)

    print_title("[Gold] Datos preparados:")
    print(df_gold.to_string(max_rows=10, min_rows=10))
    print(flush=True)

    return df_gold
