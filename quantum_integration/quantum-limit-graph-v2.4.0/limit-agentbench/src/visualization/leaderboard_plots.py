"""
Leaderboard visualization for green benchmarking.
"""

import matplotlib.pyplot as plt


def plot_accuracy_vs_energy(results, pareto):
    plt.figure(figsize=(7, 5))

    for r in results:
        plt.scatter(r["energy"], r["accuracy"], alpha=0.5)

    for r in pareto:
        plt.scatter(
            r["energy"],
            r["accuracy"],
            color="red",
            edgecolors="black",
            s=100,
        )

    plt.xlabel("Energy (Wh)")
    plt.ylabel("Accuracy")
    plt.title("Green Leaderboard: Accuracy vs Energy")
    plt.grid(True)
    plt.show()


def plot_latency_vs_energy(results):
    plt.figure(figsize=(7, 5))
    for r in results:
        plt.scatter(r["energy"], r["latency"])

    plt.xlabel("Energy (Wh)")
    plt.ylabel("Latency (s)")
    plt.title("Latency vs Energy (Pure Efficiency)")
    plt.grid(True)
    plt.show()


def plot_carbon_vs_energy(results):
    plt.figure(figsize=(7, 5))
    for r in results:
        plt.scatter(r["energy"], r["carbon"])

    plt.xlabel("Energy (Wh)")
    plt.ylabel("Carbon (kg CO₂)")
    plt.title("Carbon vs Energy (Pure Green Plot)")
    plt.grid(True)
    plt.show()
