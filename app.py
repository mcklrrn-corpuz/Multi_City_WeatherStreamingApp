"""
Streaming Data Dashboard Template
STUDENT PROJECT: Big Data Streaming Dashboard

This version supports Multi-City Comparison (Option B).
Features:
- Multi-city real-time comparison charts (temperature, humidity, pressure, wind)
- Weather icons
- Anomaly alerts per city
- Historical data loader via MongoDB
- UI improvements and comparison controls
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh

# MongoDB historical loader
from historical_data import load_historical_data

# ---------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------
st.set_page_config(
    page_title="Streaming Weather Dashboard - Multi-City",
    page_icon="🌦️",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ---------------------------------------------------------
# WEATHER ICONS FOR CONDITIONS
# ---------------------------------------------------------
def get_weather_icon(condition: str):
    """
    Returns emoji icon based on weather condition text.
    """
    condition = (condition or "").lower()

    if "sun" in condition or "clear" in condition:
        return "☀️"
    if "cloud" in condition:
        return "☁️"
    if "rain" in condition:
        return "🌧️"
    if "storm" in condition or "thunder" in condition:
        return "⛈️"
    if "wind" in condition:
        return "💨"

    return "🌍"


# ---------------------------------------------------------
# SIDEBAR CONFIG
# ---------------------------------------------------------
def setup_sidebar():
    """
    Sidebar controls including Kafka, storage and multi-city settings.
    """
    st.sidebar.title("Dashboard Controls")

    st.sidebar.subheader("Kafka Source")
    kafka_broker = st.sidebar.text_input("Kafka Broker", value="localhost:9092")
    kafka_topic = st.sidebar.text_input("Kafka Topic", value="streaming-data")

    st.sidebar.subheader("Historical Storage")
    storage_type = st.sidebar.selectbox("Storage Type", ["MongoDB", "HDFS"])
    hist_range = st.sidebar.selectbox("Historical Time Range", ["1h", "24h", "7d", "30d"])

    st.sidebar.subheader("MongoDB Connection")
    mongo_uri = st.sidebar.text_input(
        "MongoDB URI",
        value="mongodb+srv://qmlbcorpuz_db_user:cpe032@groceryinventorysystem.rh8eact.mongodb.net/?retryWrites=true&w=majority"
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("Multi-City Comparison")
    default_cities = ["Manila", "Quezon City", "Cebu", "Davao", "Baguio", "Hong Kong", "Singapore"]
    selected_cities = st.sidebar.multiselect(
        "Select cities to compare (multi-select)", 
        options=default_cities, 
        default=default_cities
    )

    metric_choice = st.sidebar.selectbox(
        "Primary Metric for Comparison",
        ["temperature_c", "humidity", "pressure_mb", "wind_kph"],
        index=0
    )

    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "storage_type": storage_type,
        "hist_range": hist_range,
        "mongo_uri": mongo_uri,
        "selected_cities": selected_cities,
        "metric_choice": metric_choice
    }
# ---------------------------------------------------------
# KAFKA REAL-TIME CONSUMER (collects recent messages)
# ---------------------------------------------------------
def consume_kafka_data(config, max_messages=200):
    kafka_broker = config.get("kafka_broker")
    kafka_topic = config.get("kafka_topic")

    cache_key = f"consumer_{kafka_broker}_{kafka_topic}"

    if cache_key not in st.session_state:
        try:
            st.session_state[cache_key] = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=[kafka_broker],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=2000
            )
        except Exception as e:
            st.error(f"Kafka connection error: {e}")
            st.session_state[cache_key] = None

    consumer = st.session_state[cache_key]
    if not consumer:
        return None

    messages = []

    try:
        msg_pack = consumer.poll(timeout_ms=1500)

        for tp, batch in msg_pack.items():
            for message in batch:
                data = message.value

                if "timestamp" not in data or "location" not in data:
                    continue

                timestamp_str = data.get("timestamp")
                if timestamp_str and timestamp_str.endswith("Z"):
                    timestamp_str = timestamp_str[:-1] + "+00:00"

                try:
                    timestamp = datetime.fromisoformat(timestamp_str) if timestamp_str else datetime.utcnow()
                except:
                    timestamp = datetime.utcnow()

                messages.append({
                    "timestamp": timestamp,
                    "location": data.get("location", ""),
                    "temperature_c": float(data.get("temperature_c", 0)),
                    "humidity": float(data.get("humidity", 0)),
                    "pressure_mb": float(data.get("pressure_mb", 0)),
                    "wind_kph": float(data.get("wind_kph", 0)),
                    "condition": data.get("condition", "")
                })

                if len(messages) >= max_messages:
                    break
            if len(messages) >= max_messages:
                break

        if not messages:
            return None

        df = pd.DataFrame(messages)
        df = df.sort_values("timestamp")
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

    except Exception as e:
        st.error(f"Error consuming Kafka messages: {e}")
        return None


# ---------------------------------------------------------
# SAMPLE DATA (fallback)
# ---------------------------------------------------------
def generate_sample_data(cities=None):
    if not cities:
        cities = ["Manila", "Quezon City", "Cebu"]

    now = datetime.utcnow()
    rows = []

    for i in range(60):
        t = now - timedelta(seconds=(60 - i) * 10)
        for city in cities:
            rows.append({
                "timestamp": t,
                "location": city,
                "temperature_c": 25 + (hash(city) % 5) + (i % 3),
                "humidity": 60 + (hash(city) % 10) + (i % 4),
                "pressure_mb": 1005 + (hash(city) % 4),
                "wind_kph": 8 + (hash(city) % 6),
                "condition": "Clear"
            })

    df = pd.DataFrame(rows)
    df = df.sort_values("timestamp")
    return df


# ---------------------------------------------------------
# UI: Latest metrics table per city
# ---------------------------------------------------------
def latest_metrics_table(df, cities):
    rows = []

    for city in cities:
        city_df = df[df["location"] == city]
        if city_df.empty:
            continue

        latest = city_df.sort_values("timestamp").iloc[-1]

        rows.append({
            "location": city,
            "timestamp": latest["timestamp"],
            "temperature_c": latest["temperature_c"],
            "humidity": latest["humidity"],
            "pressure_mb": latest["pressure_mb"],
            "wind_kph": latest["wind_kph"],
            "condition": latest["condition"]
        })

    if not rows:
        return pd.DataFrame(columns=[
            "location", "timestamp", "temperature_c",
            "humidity", "pressure_mb", "wind_kph", "condition"
        ])

    return pd.DataFrame(rows)


# ---------------------------------------------------------
# ANOMALY DETECTION
# ---------------------------------------------------------
def detect_anomalies(latest_df):
    alerts = []

    for _, row in latest_df.iterrows():
        city = row["location"]
        temp = row["temperature_c"]
        humidity = row["humidity"]
        pressure = row["pressure_mb"]
        wind = row["wind_kph"]

        heat_index = temp + 0.2 * (humidity * 0.1)

        if heat_index > 40:
            alerts.append((city, f"🔥 Extreme Heat Index: {heat_index:.1f}°C"))

        if pressure < 995:
            alerts.append((city, f"⛈️ Storm Warning: Low Pressure {pressure} mb"))

        if wind > 40:
            alerts.append((city, f"💨 Strong Winds: {wind} kph"))

    return alerts

# ---------------------------------------------------------
# REAL-TIME VIEW (Multi-City Comparison)
# ---------------------------------------------------------
def display_real_time_view(config, refresh_interval):
    st.header("🌦️ Multi-City Real-time Weather Comparison")

    with st.spinner("Fetching latest messages..."):
        df = consume_kafka_data(config)

    selected_cities = config.get("selected_cities") or []
    metric_choice = config.get("metric_choice") or "temperature_c"

    if df is None or df.empty:
        st.warning("No live Kafka messages detected — showing sample data for comparison.")
        df = generate_sample_data(cities=selected_cities)

    df = df[df["location"].isin(selected_cities)]

    if df.empty:
        st.warning("Filtered dataset is empty for the selected cities.")
        return

    # Latest metrics
    st.subheader("📊 Latest Readings (per city)")
    latest_df = latest_metrics_table(df, selected_cities)

    if not latest_df.empty:
        st.dataframe(
            latest_df.sort_values("location").reset_index(drop=True),
            width='stretch'
        )
    else:
        st.info("No latest metrics available for selected cities.")

    # Alerts
    st.subheader("⚠️ Alerts (per city)")
    alerts = detect_anomalies(latest_df)

    if alerts:
        for city, msg in alerts:
            st.error(f"{city} — {msg}")
    else:
        st.success("No anomalies detected across selected cities.")

    # Condition banner
    most_common_condition = df.groupby("location")["condition"].last().mode().tolist()
    banner_condition = most_common_condition[0] if most_common_condition else ""
    banner_icon = get_weather_icon(banner_condition)

    st.markdown(
        f"""
        <div style='padding:12px; background:#f3f8ff; border-radius:8px; margin-bottom:8px;'>
            <strong style='font-size:16px'>{banner_icon} Current condition summary: </strong>
            <span style='font-size:14px'>{banner_condition}</span>
        </div>
        """,
        unsafe_allow_html=True
    )

    # Main comparison chart
    st.subheader(f"📈 Comparison — {metric_choice.replace('_', ' ').title()}")

    try:
        fig_main = px.line(
            df,
            x="timestamp",
            y=metric_choice,
            color="location",
            title=f"{metric_choice.replace('_', ' ').title()} comparison",
            labels={metric_choice: metric_choice.replace('_', ' ').title(), "timestamp": "Time"},
            template="plotly_white"
        )
        st.plotly_chart(fig_main, width='stretch')
    except Exception as e:
        st.error(f"Error plotting main comparison: {e}")

    # Additional tiny charts
    st.subheader("🔎 Additional Comparisons")

    col1, col2 = st.columns(2)

    with col1:
        try:
            fig_h = px.line(
                df, x="timestamp", y="humidity", color="location",
                title="Humidity Comparison", template="plotly_white"
            )
            st.plotly_chart(fig_h, width='stretch')
        except:
            st.warning("Unable to render humidity chart.")

    with col2:
        try:
            fig_p = px.line(
                df, x="timestamp", y="pressure_mb", color="location",
                title="Pressure Comparison", template="plotly_white"
            )
            st.plotly_chart(fig_p, width='stretch')
        except:
            st.warning("Unable to render pressure chart.")

    # Wind chart
    try:
        fig_w = px.line(
            df, x="timestamp", y="wind_kph", color="location",
            title="Wind Speed Comparison", template="plotly_white"
        )
        st.plotly_chart(fig_w, width='stretch')
    except:
        st.warning("Unable to render wind chart.")

    # Raw incoming data
    with st.expander("📋 Raw Incoming Data (recent)"):
        st.dataframe(
            df.sort_values("timestamp", ascending=False).reset_index(drop=True),
            width='stretch'
        )


# ---------------------------------------------------------
# HISTORICAL VIEW (multi-city)
# ---------------------------------------------------------
def display_historical_view(config):
    st.header("📊 Historical Weather Comparison")

    try:
        hist_df = load_historical_data(
            uri=config["mongo_uri"],
            db_name="WeatherDB",
            collection="Readings",
            time_range=config["hist_range"]
        )
    except Exception as e:
        st.error(f"Error loading historical data: {e}")
        hist_df = None

    selected_cities = config.get("selected_cities") or []

    if hist_df is None or hist_df.empty:
        st.warning("No historical data found for the chosen range.")
        return

    hist_df["timestamp"] = pd.to_datetime(hist_df["timestamp"])
    hist_df = hist_df[hist_df["location"].isin(selected_cities)]

    if hist_df.empty:
        st.warning("No historical records found for the selected cities/time range.")
        return

    # Temperature trend
    st.subheader("Historical Trends (multi-city)")

    try:
        fig_hist_temp = px.line(
            hist_df,
            x="timestamp",
            y="temperature_c",
            color="location",
            title="Historical Temperature Comparison",
            template="plotly_white"
        )
        st.plotly_chart(fig_hist_temp, width='stretch')
    except Exception as e:
        st.error(f"Error rendering historical temperature: {e}")

    # Humidity & Pressure
    col1, col2 = st.columns(2)

    with col1:
        try:
            fig_hist_h = px.line(
                hist_df,
                x="timestamp",
                y="humidity",
                color="location",
                title="Historical Humidity Comparison",
                template="plotly_white"
            )
            st.plotly_chart(fig_hist_h, width='stretch')
        except:
            st.warning("Unable to render historical humidity chart.")

    with col2:
        try:
            fig_hist_p = px.line(
                hist_df,
                x="timestamp",
                y="pressure_mb",
                color="location",
                title="Historical Pressure Comparison",
                template="plotly_white"
            )
            st.plotly_chart(fig_hist_p, width='stretch')
        except:
            st.warning("Unable to render historical pressure chart.")

    # Wind historical
    try:
        fig_hist_w = px.line(
            hist_df,
            x="timestamp",
            y="wind_kph",
            color="location",
            title="Historical Wind Speed Comparison",
            template="plotly_white"
        )
        st.plotly_chart(fig_hist_w, width='stretch')
    except:
        st.warning("Unable to render historical wind chart.")

    # Raw historical data
    with st.expander("📄 Raw Historical Records"):
        st.dataframe(
            hist_df.sort_values("timestamp", ascending=True).reset_index(drop=True),
            width='stretch'
        )

# ---------------------------------------------------------
# MAIN APP
# ---------------------------------------------------------
def main():
    st.title("🌍 Multi-City Weather Streaming Dashboard")

    # Initialize refresh state
    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {
            'last_refresh': datetime.utcnow(),
            'auto_refresh': True
        }

    # Load sidebar config
    config = setup_sidebar()

    # Auto-refresh controls
    st.sidebar.subheader("Auto-refresh Settings")
    st.session_state.refresh_state["auto_refresh"] = st.sidebar.checkbox(
        "Enable Auto Refresh",
        value=True
    )

    refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 10)

    if st.session_state.refresh_state["auto_refresh"]:
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")

    # Manual refresh
    if st.sidebar.button("🔄 Manual Refresh"):
        st.rerun()

    # Tabs for the two views
    tab1, tab2 = st.tabs(["🌦️ Real-time Comparison", "📊 Historical Comparison"])

    with tab1:
        display_real_time_view(config, refresh_interval)

    with tab2:
        display_historical_view(config)


if __name__ == "__main__":
    main()
