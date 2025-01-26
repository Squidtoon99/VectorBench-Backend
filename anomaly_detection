import json
import pandas as pd
from sklearn.ensemble import IsolationForest
import plotly.graph_objects as go
import numpy as np
import os
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

TS_HOST = os.getenv('TS_HOST')
TS_PORT = os.getenv('TS_PORT', 5432)
TS_NAME = os.getenv('TS_DATABASE')
TS_USER = os.getenv('TS_USER')
TS_PASSWORD = os.getenv('TS_PASSWORD')

conn = psycopg2.connect(
        host=TS_HOST,
        port=TS_PORT,
        dbname=TS_NAME,
        user=TS_USER,
        password=TS_PASSWORD
    )
cursor = conn.cursor()

def detect_anomalies(ticker):
    cursor.execute("SELECT * FROM stocks WHERE ticker = %s", (ticker,))

    results = cursor.fetchall()

    columns = [
        'id', 't', 'o', 'h', 'l', 'n', 'c', 'v', 'vw', 'ticker'
    ]
    df = pd.DataFrame(results, columns=columns)
    df = df.drop(columns=['id', 'ticker'])

    df["t"] = pd.to_datetime(df["t"])
    df.sort_values(by="t", inplace=True)
    df.reset_index(drop=True, inplace=True)

    df['price_change'] = df['c'].diff()
    df['percentage_change'] = df['c'].pct_change()
    X = df[['o', 'h', 'l', 'c', 'v', 'vw', 'price_change', 'percentage_change']].dropna()

    iso_forest = IsolationForest(
        n_estimators=100,
        contamination=0.07,  # 7% anomalies
        random_state=42
    )

    iso_forest.fit(X)
    labels = iso_forest.predict(X)
    df = df.iloc[len(df) - len(X):]  # Adjust for dropped rows due to diff and pct_change
    df['anomaly_label'] = labels

    anomalies = df[df["anomaly_label"] == -1]
    print("\n=== Anomalies Found ===")
    print("Indices:\n", anomalies.index.tolist())
    print("\nData Points:\n", anomalies)

    df['mid_point'] = (df['h'] + df['l']) / 2

    # Combine points within 24 hours
    time_threshold = pd.Timedelta(hours=24)

    anomalies = df[df["anomaly_label"] == -1].copy()
    clusters = []
    current_cluster = []

    for i, row in anomalies.iterrows():
        if not current_cluster:
            current_cluster.append(row)
        else:
            if row["t"] - current_cluster[-1]["t"] <= time_threshold:
                current_cluster.append(row)
            else:
                clusters.append(current_cluster)
                current_cluster = [row]

    if current_cluster:
        clusters.append(current_cluster)

    points = []
    for cluster in clusters:
        cluster_df = pd.DataFrame(cluster)
        time = cluster_df["t"].median().round("H")  # Round to the nearest hour
        mp = cluster_df["mid_point"].mean()
        points.append((time, mp))

    display_data(df, points, ticker)

    return points

def display_data(df, points, ticker):
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=df['t'],
                open=df['o'],
                high=df['h'],
                low=df['l'],
                close=df['c'],
                name=ticker
            )
        ]
    )

    fig.add_trace(go.Scatter(
        x=[pt[0] for pt in points],
        y=[pt[1] for pt in points],
        mode="markers",
        name="Anomaly",
        marker=dict(color="black", size=10, symbol="x")
    ))

    fig.update_layout(
        title=f"{ticker} Close Price with Anomalies (Combined)",
        xaxis_title="Time",
        yaxis_title="Price (USD)",
        xaxis_rangeslider_visible=False,
        hovermode="x unified"
    )

    fig.show()

def upload_to_db(points, ticker):
    # Prepare the data for insertion
    rows = [
        (
            pt[0],  # trade_time
            ticker,  # ticker
        )
        for pt in points
    ]

    # SQL insert statement
    sql = """
    INSERT INTO anomaly (trade_time, ticker)
    VALUES %s
    ON CONFLICT (trade_time, ticker) DO NOTHING;
    """

    # Use execute_values for batch insert
    execute_values(cursor, sql, rows)

    # Commit the transaction and close the connection
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Uploaded {len(rows)} rows to the database.")

def main():
    ticker = "TSLA"

    anomalies = detect_anomalies(ticker)
    upload_to_db(anomalies, ticker)

if __name__ == '__main__':
    main()
