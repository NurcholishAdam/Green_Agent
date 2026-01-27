# src/visualization/pareto_plots.py

import matplotlib.pyplot as plt


def plot_accuracy_vs_carbon(results):
    for r in results:
        plt.scatter(r["carbon"], r["accuracy"])
    plt.xlabel("Carbon (kg CO2)")
    plt.ylabel("Accuracy")
    plt.title("Accuracy vs Carbon")
    plt.show()


def plot_latency_vs_energy(results):
    for r in results:
        plt.scatter(r["energy"], r["latency"])
    plt.xlabel("Energy (Wh)")
    plt.ylabel("Latency (s)")
    plt.title("Latency vs Energy")
    plt.show()
