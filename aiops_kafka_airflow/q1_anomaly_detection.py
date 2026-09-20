import os
import pandas as pd
import matplotlib.pyplot as plt

DATA_FILE = "data/server_metrics.csv"
PLOT_FILE = "data/anomaly_plot.png"

CPU_THRESHOLD = 80
MEMORY_THRESHOLD = 90
RESPONSE_TIME_THRESHOLD = 300


def main():
    df = pd.read_csv(DATA_FILE)

    print("=" * 50)
    print("AIOps Log Anomaly Detection")
    print("=" * 50)

    print(f"Total records: {len(df)}")

    print("\nBasic statistics:")
    print(df[["cpu_usage", "memory_usage", "response_time"]].describe())

    # Simple threshold-based anomaly detection.
    df["anomaly"] = (
        (df["cpu_usage"] > CPU_THRESHOLD)
        | (df["memory_usage"] > MEMORY_THRESHOLD)
        | (df["response_time"] > RESPONSE_TIME_THRESHOLD)
    )

    anomalies = df[df["anomaly"]]

    print(f"\nAnomalies detected: {len(anomalies)}")
    print("\nAnomalous records:")
    if len(anomalies):
        print(
            anomalies[
                ["timestamp", "cpu_usage", "memory_usage", "response_time"]
            ].to_string(index=False)
        )
    else:
        print("No anomalies detected.")

    # Graph CPU values and highlight anomalies.
    plt.figure(figsize=(10, 5))
    plt.plot(df["timestamp"], df["cpu_usage"], marker="o", label="CPU Usage")
    plt.scatter(
        anomalies["timestamp"],
        anomalies["cpu_usage"],
        s=90,
        label="Anomaly",
    )
    plt.axhline(CPU_THRESHOLD, linestyle="--", label="CPU threshold (80%)")
    plt.xlabel("Timestamp")
    plt.ylabel("CPU Usage (%)")
    plt.title("AIOps CPU Anomaly Detection")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()

    os.makedirs(os.path.dirname(PLOT_FILE), exist_ok=True)
    plt.savefig(PLOT_FILE)
    plt.close()

    print(f"\nGraph saved to: {PLOT_FILE}")


if __name__ == "__main__":
    main()
