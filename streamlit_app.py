import requests
import streamlit as st
import pandas as pd
import folium
from folium.plugins import MarkerCluster
import branca.colormap as cm
from pandas import json_normalize
import asyncio
from playwright.async_api import async_playwright
from streamlit_folium import st_folium
from streamlit_autorefresh import st_autorefresh
import altair as alt
import datetime


st.set_page_config(layout="wide")

# Auto-refresh cada 5 minutos (solo una vez)
st_autorefresh(interval=300000, limit=1, key="autorefresh")

st.title("🌊 RiasViewer")

# ---------- FUNCIONES ----------

async def scrape_and_flatten_json(url):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.set_extra_http_headers({"User-Agent": "Mozilla/5.0"})
            response = await page.goto(url)
            if not response or response.status != 200:
                raise Exception(f"HTTP status: {response.status if response else 'no response'}")
            data = await response.json()
            await browser.close()

            key = 'data'
            if key in data:
                df = json_normalize(data[key])
            else:
                df = json_normalize(data)

            df_lista = []
            for i in df.columns:
                valor = df.loc[0, i]
                if isinstance(valor, list) and len(valor) > 0 and isinstance(valor[0], dict):
                    df_temp = pd.DataFrame(valor)
                    df_lista.append(df_temp)

            return pd.concat(df_lista, ignore_index=True) if df_lista else pd.DataFrame()

    except Exception as e:
        return pd.DataFrame()

def run_scraping_sync(url):
    try:
        return asyncio.run(scrape_and_flatten_json(url))
    except RuntimeError:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(scrape_and_flatten_json(url))

# ---------- PARÁMETROS DE MAPA ----------
X_min = 1946
X_max = 1950
Y_min = 1516
Y_max = 1518
zoom = 13

# ---------- CARGAR DATOS (con cache 5 min) ----------
@st.cache_data(ttl=300)
def get_data(x_min, x_max, y_min, y_max, zoom):
    df_list = []
    total_urls = (x_max - x_min + 1) * (y_max - y_min + 1)
    count = 0
    progress = st.progress(0, text="Cargando datos...")

    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            url = f'https://www.marinetraffic.com/getData/get_data_json_4/z:{zoom}/X:{x}/Y:{y}/station:0'
            df = run_scraping_sync(url)
            if not df.empty:
                df_list.append(df)
            count += 1
            progress.progress(count / total_urls, text=f"{count}/{total_urls} coordenadas procesadas...")

    progress.empty()

    if df_list:
        df_final = pd.concat(df_list, ignore_index=True)
        df_final['LAT'] = pd.to_numeric(df_final['LAT'], errors='coerce')
        df_final['LON'] = pd.to_numeric(df_final['LON'], errors='coerce')
        df_final['SPEED'] = pd.to_numeric(df_final['SPEED'], errors='coerce') / 10
        df_final = df_final.dropna(subset=['LAT', 'LON', 'SPEED'])
        return df_final
    else:
        return pd.DataFrame()

# ---------- OBTENER DATOS ----------
df_final = get_data(X_min, X_max, Y_min, Y_max, zoom)

if not df_final.empty:
    st.session_state['last_run'] = datetime.datetime.now()
    st.success(f"✅ {len(df_final)} barcos encontrados.")
    st.session_state['df_final'] = df_final
else:
    st.warning("No existen datos válidos.")

if 'last_run' in st.session_state:
    st.markdown(f"Última ejecución exitosa: **{st.session_state['last_run'].strftime('%Y-%m-%d %H:%M:%S')}**")
else:
    st.markdown("No hay registro de ejecución previa.")

# ---------- FILTROS ----------
if not df_final.empty:
    st.subheader("⚙️ Filtros de visualización")
    filtro_estado = st.radio(
        "Mostrar:",
        ["Todos los barcos", "Solo en movimiento", "Solo atracados"],
        horizontal=True
    )

    if filtro_estado == "Solo en movimiento":
        df_final = df_final[df_final['SPEED'] >= 5]
    elif filtro_estado == "Solo atracados":
        df_final = df_final[df_final['SPEED'] < 5]

    # ---------- ESTADÍSTICAS ----------
    st.subheader("📊 Estadísticas de tráfico marítimo")
    col1, col2, col3 = st.columns(3)
    col1.metric("🚢 Barcos mostrados", len(df_final))
    col2.metric("📈 Velocidad media", f"{df_final['SPEED'].mean():.2f} knots")
    col3.metric("🐢 Barcos lentos (<5 knots)", len(df_final[df_final['SPEED'] < 5]))

    if (df_final['SPEED'] > 30).any():
        st.warning("🚨 Hay barcos con velocidad mayor a 30 knots.")
    if (df_final['SPEED'] == 0).any():
        st.info("⚓ Algunos barcos están completamente detenidos.")

    # ---------- GRÁFICO DE VELOCIDAD ----------
    st.subheader("📉 Distribución de velocidad")
    chart = alt.Chart(df_final).mark_bar().encode(
        alt.X("SPEED:Q", bin=alt.Bin(maxbins=30), title="Velocidad (knots)"),
        alt.Y("count()", title="Número de barcos")
    ).properties(width=700, height=300)
    st.altair_chart(chart)

    # ---------- MAPA ----------
    st.subheader("🗌️ Mapa de barcos")
    mapa = folium.Map(
        location=[df_final['LAT'].mean(), df_final['LON'].mean()],
        zoom_start=12,
        tiles=None
    )

    folium.TileLayer(
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='Esri', name='Satélite (Esri)', overlay=True, control=True, show=True
    ).add_to(mapa)

    folium.TileLayer('CartoDB positron', name='Mapa claro', overlay=False, control=True).add_to(mapa)

    layer_speed = folium.FeatureGroup(name='Barcos (Velocidad)', overlay=True, show=False)
    layer_slow = folium.FeatureGroup(name='Barcos Atracados', overlay=True, show=False)
    layer_moving = folium.FeatureGroup(name='Barcos en Movimiento', overlay=True, show=False)
    marker_cluster = MarkerCluster(name='Cluster de barcos', overlay=True).add_to(mapa)

    colormap = cm.LinearColormap(['green', 'yellow', 'red'],
                                 vmin=df_final['SPEED'].min(),
                                 vmax=df_final['SPEED'].max())
    colormap.caption = 'Velocidad del barco (knots)'
    colormap.add_to(mapa)

    for _, row in df_final.iterrows():
        try:
            name = row.get('SHIPNAME', 'Desconocido')
            speed = row['SPEED']

            folium.CircleMarker(
                location=[row['LAT'], row['LON']],
                radius=6,
                color=colormap(speed),
                fill=True,
                fill_color=colormap(speed),
                fill_opacity=0.8,
                popup=f"🚢 {name}<br>⚡ {speed:.2f} knots"
            ).add_to(layer_speed)

            if speed < 5:
                folium.Circle(
                    location=[row['LAT'], row['LON']],
                    radius=100,
                    color='orange',
                    fill=True,
                    fill_color='orange',
                    fill_opacity=0.6,
                    popup=f"🐢 Barco lento<br>🚢 {name}<br>⚡ {speed:.2f} knots"
                ).add_to(layer_slow)
            else:
                folium.CircleMarker(
                    location=[row['LAT'], row['LON']],
                    radius=7,
                    color='lime',
                    fill=True,
                    fill_color='lime',
                    fill_opacity=0.9,
                    popup=f"🚀 Barco en movimiento<br>🚢 {name}<br>⚡ {speed:.2f} knots"
                ).add_to(layer_moving)

            folium.Marker(
                location=[row['LAT'], row['LON']],
                popup=f"🚢 {name}<br>⚡ {speed:.2f} knots",
                icon=folium.Icon(color='green', icon='anchor', prefix='fa')
            ).add_to(marker_cluster)
        except Exception as e:
            st.warning(f"Error al agregar marcador: {e}")

    mapa.add_child(layer_speed)
    mapa.add_child(layer_slow)
    mapa.add_child(layer_moving)
    folium.LayerControl(collapsed=False).add_to(mapa)

    st_folium(mapa, use_container_width=True, height=600)

    # ---------- TABLA Y DESCARGA ----------
    with st.expander("📋 Ver tabla de barcos", expanded=True):
        columnas_mostrar = ['SHIPNAME', 'SPEED', 'COURSE', 'SHIPTYPE', 'DESTINATION', 'FLAG']
        columnas_mostrar = [col for col in columnas_mostrar if col in df_final.columns]

        nombres_columnas = {
            'SHIPNAME': 'Nombre del barco',
            'SPEED': 'Velocidad (knots)',
            'COURSE': 'Dirección (°)',
            'SHIPTYPE': 'Tipo de barco',
            'DESTINATION': 'Destino',
            'FLAG': 'Bandera'
        }

        df_mostrar = df_final[columnas_mostrar].rename(columns=nombres_columnas)
        st.dataframe(df_mostrar, use_container_width=True, height=1200)

    st.subheader("⬇️ Descargar datos")
    csv = df_mostrar.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📅 Descargar CSV",
        data=csv,
        file_name="barcos_marine_traffic.csv",
        mime='text/csv'
    )
