# src/visualization/pareto_plots.py

import matplotlib.pyplot as plt


def plot_accuracy_vs_carbon(results):
    for r in results:
        plt.scatter(r["carbon"], r["accuracy"])
    plt.xlabel("Carbon (kg)")
    plt.ylabel("Accuracy")
    plt.show()
