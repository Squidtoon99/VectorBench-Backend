import math
import numpy as np
from fastdtw import fastdtw
import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from psycopg2.extras import execute_values



# Load environment variables
load_dotenv()

# Database connection parameters
TS_HOST = os.getenv('TS_HOST')
TS_PORT = os.getenv('TS_PORT', 5432)
TS_NAME = os.getenv('TS_DATABASE')
TS_USER = os.getenv('TS_USER')
TS_PASSWORD = os.getenv('TS_PASSWORD')

# Establish connection to the database
conn = psycopg2.connect(
    host=TS_HOST,
    port=TS_PORT,
    dbname=TS_NAME,
    user=TS_USER,
    password=TS_PASSWORD
)
cursor = conn.cursor()

# Load bars from the database for a specific ticker
def load_bars_from_db(ticker):
    query = "SELECT * FROM stocks WHERE ticker = %s ORDER BY trade_time;"
    cursor.execute(query, (ticker,))
    results = cursor.fetchall()

    columns = [
        'id', 'trade_time', 'o', 'h', 'l', 'n', 'c', 'v', 'vw', 'ticker'
    ]
    df = pd.DataFrame(results, columns=columns)
    df = df.drop(columns=['id', 'ticker'])
    return df

# Load anomalies from the anomaly table
def load_anomalies_from_db(ticker):
    query = "SELECT trade_time FROM anomaly WHERE ticker = %s ORDER BY trade_time;"
    cursor.execute(query, (ticker,))
    results = cursor.fetchall()

    # Extract trade_time values as anomaly indices
    anomaly_times = [row[0] for row in results]
    return anomaly_times

# Convert a single bar to a feature vector
def bar_to_vector(bar):
    return np.array([
        bar["o"],
        bar["h"],
        bar["l"],
        bar["c"],
        bar["v"],
        bar["n"],
        bar["vw"]
    ], dtype=float)

# Calculate the angle-based distance between two vectors
def angle_distance(vec_a, vec_b):
    norm_a = np.linalg.norm(vec_a)
    norm_b = np.linalg.norm(vec_b)
    if norm_a < 1e-12 or norm_b < 1e-12:
        return 0.0

    dot_product = np.dot(vec_a, vec_b)
    cos_value = dot_product / (norm_a * norm_b)
    # Clamp to avoid floating precision issues outside [-1, 1]
    cos_value = max(min(cos_value, 1.0), -1.0)
    return math.acos(cos_value)

# Calculate the global distance using FastDTW
def calc_global_distance(ticker1, ticker2):
    bars1 = load_bars_from_db(ticker1)
    bars2 = load_bars_from_db(ticker2)

    vectors1 = bars1.apply(bar_to_vector, axis=1).tolist()
    vectors2 = bars2.apply(bar_to_vector, axis=1).tolist()

    distance, path = fastdtw(vectors1, vectors2, dist=angle_distance)
    print(f"Raw DTW Distance = {distance}")

def update_anomalies_in_db(anomalies, ticker1, ticker2):
    rows = [
        (ticker2, distance, anomaly_time, ticker1)  # Values for bot, distance, trade_time, and ticker
        for anomaly_time, distance in anomalies
    ]

    query = """
    UPDATE anomaly
    SET bot = %s, distance = %s
    WHERE trade_time = %s AND ticker = %s;
    """

    cursor.executemany(query, rows)
    conn.commit()
    print(f"Updated {len(rows)} anomalies in the database.")


# Calculate anomaly distances using Euclidean distance with nearest points
def calc_anomaly_distance(ticker1, ticker2):
    bars1 = load_bars_from_db(ticker1)
    bars2 = load_bars_from_db(ticker2)

    # Load all anomaly times
    anomaly_times = load_anomalies_from_db(ticker1)

    # Extract trade times and closing prices for both tickers
    times1 = bars1['trade_time'].tolist()
    prices1 = bars1['c'].tolist()
    times2 = bars2['trade_time'].tolist()
    prices2 = bars2['c'].tolist()

    anomaly_data = []

    for anomaly_time in anomaly_times:
        if anomaly_time in times1:
            anomaly_idx = times1.index(anomaly_time)
        else:
            # Find the closest time in ticker1 if the exact anomaly_time is not found
            closest_time_idx = min(range(len(times1)), key=lambda i: abs(times1[i] - anomaly_time))
            anomaly_time = times1[closest_time_idx]
            anomaly_idx = closest_time_idx

        anomaly_price = prices1[anomaly_idx]

        # Find the nearest trade time in ticker2
        nearest_time_idx = min(range(len(times2)), key=lambda i: abs(times2[i] - anomaly_time))
        nearest_price = prices2[nearest_time_idx]

        # Calculate Euclidean distance
        distance = abs(anomaly_price - nearest_price)
        anomaly_data.append((anomaly_time, distance))

    # Insert anomalies into the database
    update_anomalies_in_db(anomaly_data, ticker1, ticker2)

    # Plot the charts with anomalies
    plot_with_anomalies(bars1, bars2, [anomaly[0] for anomaly in anomaly_data])

# Plot the comparison with anomalies highlighted
def plot_with_anomalies(bars1, bars2, anomalies):
    plt.figure(figsize=(12, 6))
    plt.plot(
        bars1['trade_time'], bars1['c'], label="Ticker 1 (Closing Price)", color="blue", alpha=0.7
    )
    plt.plot(
        bars2['trade_time'], bars2['c'], label="Ticker 2 (Closing Price)", color="orange", alpha=0.7
    )

    # Highlight anomalies
    for anomaly in anomalies:
        plt.axvline(x=anomaly, color="red", linestyle="--", label=f"Anomaly @ {anomaly}")

    plt.title("Comparison of Closing Prices with Anomalies")
    plt.xlabel("Trade Time")
    plt.ylabel("Closing Price")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# Main function
def main():
    ticker1 = "TSLA"
    ticker2 = "TSLA-random"  # Replace with another ticker from your dataset

    # Calculate global distance
    calc_global_distance(ticker1, ticker2)

    # Calculate anomaly distances and save to the database
    calc_anomaly_distance(ticker1, ticker2)

if __name__ == "__main__":
    main()

