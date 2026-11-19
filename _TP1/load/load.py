import plotly.graph_objects as go
import time

from app.functions import NOMBRES_MESES, configure_pandas, print_title
from pathlib import Path

def run_gold(df_silver):
    configure_pandas()

    Path("outputs").mkdir(exist_ok=True)

    df_gold = load_gold(df_silver)
    time.sleep(0.3)

    df_cierres_anuales = load_gold_cierres_anuales(df_gold)
    load_gold_cierres_anuales_disclaimer()
    time.sleep(0.3)

    df_rendimientos_anuales = load_gold_rendimientos_anuales(df_cierres_anuales)
    load_gold_rendimientos_anuales_grafico(df_rendimientos_anuales)
    time.sleep(0.3)

    df_rendimiento_acumulado = load_gold_rendimiento_acumulado(df_cierres_anuales)
    load_gold_rendimiento_acumulado_grafico(df_rendimiento_acumulado)
    time.sleep(0.3)

    df_correlaciones = load_gold_correlaciones(df_gold)
    load_gold_correlaciones_grafico(df_correlaciones)
    time.sleep(0.3)

    df_resumen_mercados = load_gold_mercados(df_rendimientos_anuales)
    load_gold_mercados_grafico(df_resumen_mercados)
    time.sleep(0.3)

    df_tendencia_reciente = load_gold_tendencia(df_rendimientos_anuales)
    load_gold_tendencia_grafico(df_tendencia_reciente)
    time.sleep(0.3)

    df_volumen_relativo = load_gold_volumen_mensual(df_gold)
    time.sleep(0.3)

    df_resumen_volumen = load_gold_volumen_promedio(df_volumen_relativo)
    df_resumen_frecuencia_volumen = load_gold_frecuencia_volumen(df_volumen_relativo)
    load_gold_frecuencia_volumen_grafico(
        df_resumen_volumen,
        df_resumen_frecuencia_volumen
    )
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

def load_gold_cierres_anuales_disclaimer():
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

def load_gold_rendimientos_anuales_grafico(df_rendimientos_anuales):
    # Creo una copia de los rendimientos anuales.
    df_grafico_anual = df_rendimientos_anuales.copy()

    # Identifico los mejores y peores años de cada índice bursátil.
    df_mejores_anios = df_grafico_anual.loc[
        df_grafico_anual.groupby("Ticker")["Rendim. anual (%)"].idxmax()
    ].copy()

    df_peores_anios = df_grafico_anual.loc[
        df_grafico_anual.groupby("Ticker")["Rendim. anual (%)"].idxmin()
    ].copy()

    # Creo un resumen para el gráfico.
    df_resumen_anual = df_mejores_anios[
        ["País", "Índice", "Año", "Rendim. anual (%)"]
    ].merge(
        df_peores_anios[
            ["País", "Índice", "Año", "Rendim. anual (%)"]
        ],
        on=["País", "Índice"],
        suffixes=("_mejor", "_peor")
    ).sort_values("País")

    # Creo un gráfico comparativo de los mejores y peores años.
    fig = go.Figure()

    fig.add_trace(go.Bar(
        name="Mejor año",
        x=df_resumen_anual["País"],
        y=df_resumen_anual["Rendim. anual (%)_mejor"],
        text=df_resumen_anual["Año_mejor"],
        textposition="outside",
        marker_color="green",
        hovertemplate="%{x}<br>Año: %{text}<br>Rendimiento: %{y:.2f}%<extra></extra>"
    ))

    fig.add_trace(go.Bar(
        name="Peor año",
        x=df_resumen_anual["País"],
        y=df_resumen_anual["Rendim. anual (%)_peor"],
        text=df_resumen_anual["Año_peor"],
        textposition="outside",
        marker_color="red",
        hovertemplate="%{x}<br>Año: %{text}<br>Rendimiento: %{y:.2f}%<extra></extra>"
    ))

    fig.update_layout(
        title="Mejores y peores años de los índices bursátiles (2007-2025)",
        title_x=0.5,
        xaxis_title="País",
        yaxis_title="Rendimiento anual (%)",
        barmode="group",
        height=600,
        template="plotly_white",
        xaxis_tickangle=-45,
        xaxis_categoryorder="array",
        xaxis_categoryarray=df_resumen_anual["País"].tolist()
    )

    ruta_png = "outputs/grafico_anual.png"
    fig.write_image(ruta_png, format="png", scale=2)
    print(f'* Gráfico generado correctamente: "{ruta_png}".', flush=True)

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

def load_gold_rendimiento_acumulado_grafico(df_rendimiento_acumulado):
    # Creo una copia de los rendimientos acumulados para el gráfico.
    df_grafico_acumulado = df_rendimiento_acumulado.copy()

    # Ordeno los países de mayor a menor rendimiento acumulado.
    df_grafico_acumulado = df_grafico_acumulado.sort_values(
        "Rendim. acumulado (%)", ascending=False
    )

    # Creo el gráfico comparativo del rendimiento acumulado.
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df_grafico_acumulado["Rendim. acumulado (%)"],
        y=df_grafico_acumulado["País"],
        orientation="h",
        marker_color=[
            "green" if valor >= 0 else "red"
            for valor in df_grafico_acumulado["Rendim. acumulado (%)"]
        ],
        text=df_grafico_acumulado["Rendim. acumulado (%)"].map(
            lambda valor: f"{valor:.2f}%"
        ),
        textposition="outside",
        cliponaxis=False,
        hovertemplate="%{y}<br>Rendimiento acumulado: %{x:.2f}%<extra></extra>"
    ))

    fig.update_layout(
        title="Rendimiento acumulado de los índices bursátiles (2007-2025)",
        title_x=0.5,
        xaxis_title="Rendimiento acumulado (%)",
        yaxis_title="País",
        height=700,
        template="plotly_white",
        yaxis_autorange="reversed",
        showlegend=False
    )

    ruta_png = "outputs/grafico_acumulado.png"
    fig.write_image(ruta_png, format="png", scale=2)
    print(f'* Gráfico generado correctamente: "{ruta_png}".', flush=True)

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

def load_gold_correlaciones_grafico(df_correlaciones):
    # Defino el continente de cada país.
    continentes = {
        "Alemania": "Europa",
        "Australia": "Oceanía",
        "Brasil": "América",
        "Canadá": "América",
        "China": "Asia",
        "Corea del Sur": "Asia",
        "España": "Europa",
        "Estados Unidos": "América",
        "Francia": "Europa",
        "Hong Kong": "Asia",
        "India": "Asia",
        "Japón": "Asia",
        "México": "América",
        "Nueva Zelanda": "Oceanía",
        "Países Bajos": "Europa",
        "Reino Unido": "Europa",
        "Suiza": "Europa",
        "Taiwán": "Asia"
    }

    # Creo una copia de las correlaciones calculadas en Gold.
    df_correlaciones_continentes = df_correlaciones.copy()

    # Calculo la correlación promedio dentro de cada continente y entre continentes.
    regiones = ["América", "Asia", "Europa", "Oceanía"]
    matriz = []

    for region_1 in regiones:
        fila = []

        for region_2 in regiones:
            paises_1 = [
                pais for pais in df_correlaciones_continentes.index
                if continentes[pais] == region_1
            ]
            paises_2 = [
                pais for pais in df_correlaciones_continentes.columns
                if continentes[pais] == region_2
            ]

            valores = [
                df_correlaciones_continentes.loc[pais_1, pais_2]
                for pais_1 in paises_1
                for pais_2 in paises_2
                if pais_1 != pais_2
            ]

            fila.append(round(sum(valores) / len(valores), 2))

        matriz.append(fila)

    # Creo el mapa de calor con tonos verdes según la correlación.
    fig = go.Figure(go.Heatmap(
        z=matriz,
        x=regiones,
        y=regiones,
        colorscale="Greens",
        zmin=0,
        zmax=1,
        text=matriz,
        texttemplate="%{text:.2f}",
        textfont=dict(size=18),
        colorbar=dict(title="Correlación"),
        hovertemplate="%{y} / %{x}<br>Correlación: %{z:.2f}<extra></extra>"
    ))

    fig.update_layout(
        title="Correlación promedio entre continentes (2007-2025)",
        title_x=0.5,
        xaxis_title="Continente",
        yaxis_title="Continente",
        height=550,
        template="plotly_white",
        yaxis_autorange="reversed"
    )

    ruta_png = "outputs/grafico_correlaciones.png"
    fig.write_image(ruta_png, format="png", scale=2)
    print(f'* Gráfico generado correctamente: "{ruta_png}".', flush=True)

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

def load_gold_mercados_grafico(df_resumen_mercados):
    # Creo una copia de los indicadores de mercados desarrollados y emergentes.
    df_grafico_mercados = df_resumen_mercados.copy()

    # Creo un gráfico para comparar el rendimiento y la volatilidad.
    fig = go.Figure()

    fig.add_trace(go.Bar(
        name="Rendimiento anual promedio",
        x=df_grafico_mercados["Mercado"],
        y=df_grafico_mercados["Rendimiento anual promedio (%)"],
        marker_color="green",
        text=df_grafico_mercados["Rendimiento anual promedio (%)"].map(
            lambda valor: f"{valor:.2f}%"
        ),
        textposition="outside"
    ))

    fig.add_trace(go.Bar(
        name="Volatilidad anual",
        x=df_grafico_mercados["Mercado"],
        y=df_grafico_mercados["Volatilidad anual promedio (%)"],
        marker_color="orange",
        text=df_grafico_mercados["Volatilidad anual promedio (%)"].map(
            lambda valor: f"{valor:.2f}%"
        ),
        textposition="outside"
    ))

    fig.update_layout(
        title="Rendimiento y volatilidad por tipo de mercado (2007-2025)",
        title_x=0.5,
        xaxis_title="Tipo de mercado",
        yaxis_title="Porcentaje (%)",
        barmode="group",
        height=500,
        template="plotly_white"
    )

    ruta_png = "outputs/grafico_mercados.png"
    fig.write_image(ruta_png, format="png", scale=2)
    print(f'* Gráfico generado correctamente: "{ruta_png}".', flush=True)

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

def load_gold_tendencia_grafico(df_tendencia_reciente):
    # Defino los códigos internacionales de los 18 países.
    codigos_paises = {
        "Alemania": "DEU",
        "Australia": "AUS",
        "Brasil": "BRA",
        "Canadá": "CAN",
        "China": "CHN",
        "Corea del Sur": "KOR",
        "España": "ESP",
        "Estados Unidos": "USA",
        "Francia": "FRA",
        "Hong Kong": "HKG",
        "India": "IND",
        "Japón": "JPN",
        "México": "MEX",
        "Nueva Zelanda": "NZL",
        "Países Bajos": "NLD",
        "Reino Unido": "GBR",
        "Suiza": "CHE",
        "Taiwán": "TWN"
    }

    # Creo una copia y ordeno los países por rendimiento ponderado.
    df_ranking_ponderado = df_tendencia_reciente.copy()
    df_ranking_ponderado = df_ranking_ponderado.sort_values(
        "Rendimiento ponderado (%)", ascending=False
    ).reset_index(drop=True)

    # Creo el planisferio con el rendimiento ponderado de cada país.
    fig = go.Figure(go.Choropleth(
        locations=df_ranking_ponderado["País"].map(codigos_paises),
        z=df_ranking_ponderado["Rendimiento ponderado (%)"],
        customdata=df_ranking_ponderado["País"],
        locationmode="ISO-3",
        colorscale=[[0, "blue"], [1, "green"]],
        zmin=0,
        zmax=df_ranking_ponderado["Rendimiento ponderado (%)"].max(),
        marker_line_color="white",
        marker_line_width=0.5,
        colorbar_title="Rendimiento (%)",
        hovertemplate="País: %{customdata}<br>Rendimiento anual promedio ponderado: %{z:.2f}%<extra></extra>"
    ))

    fig.update_geos(
        projection_type="natural earth",
        showcoastlines=False,
        showcountries=True,
        countrycolor="white",
        showland=True,
        landcolor="lightgray",
        showframe=False
    )

    # Agrego el ranking en el espacio izquierdo del gráfico.
    fig.add_annotation(
        x=0.40,
        y=0.50,
        xref="paper",
        yref="paper",
        text="<b>Ranking:</b><br><br>" + "<br>".join(
            f"{posicion + 1}. {fila['País']}: {fila['Rendimiento ponderado (%)']:.2f}%"
            for posicion, fila in df_ranking_ponderado.iterrows()
        ),
        showarrow=False,
        xanchor="left",
        yanchor="top",
        align="left",
        font=dict(size=10)
    )

    fig.update_layout(
        title="Rendimiento anual promedio ponderado por país (2007-2025)",
        title_x=0.5,
        height=650,
        dragmode=False,
        margin=dict(b=10, l=10, r=10, t=80),
        template="plotly_white"
    )

    ruta_png = "outputs/grafico_tendencia.png"
    fig.write_image(ruta_png, format="png", scale=2)
    print(f'* Gráfico generado correctamente: "{ruta_png}".', flush=True)

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

def load_gold_frecuencia_volumen_grafico(
    df_resumen_volumen,
    df_resumen_frecuencia_volumen
):
    # Creo un gráfico de barras con la variación mensual del volumen de operaciones.
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df_resumen_volumen["Mes"],
        y=df_resumen_volumen["Variación del volumen (%)"],
        marker_color=[
            "green" if valor >= 0 else "red"
            for valor in df_resumen_volumen["Variación del volumen (%)"]
        ],
        text=df_resumen_volumen["Variación del volumen (%)"].map(
            lambda valor: f"{valor:.2f}%"
        ),
        textposition="outside",
        customdata=df_resumen_frecuencia_volumen[
            "Frecuencia de menor actividad (%)"
        ],
        hovertemplate="Mes: %{x}<br>Variación del volumen: %{y:.2f}%<br>Frecuencia de menor actividad: %{customdata:.2f}%<extra></extra>"
    ))

    fig.update_layout(
        title="Variación mensual del volumen bursátil respecto del promedio anual (2007-2025)",
        title_x=0.5,
        xaxis_title="Mes",
        yaxis_title="Variación del volumen (%)",
        height=550,
        template="plotly_white",
        showlegend=False,
        yaxis_zeroline=True,
        yaxis_zerolinecolor="black"
    )

    ruta_png = "outputs/grafico_volumen.png"
    fig.write_image(ruta_png, format="png", scale=2)
    print(f'* Gráfico generado correctamente: "{ruta_png}".', flush=True)

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
