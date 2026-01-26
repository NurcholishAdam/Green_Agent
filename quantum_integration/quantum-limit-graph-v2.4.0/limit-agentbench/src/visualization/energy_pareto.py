"""
Visualization utilities for green Pareto analysis.
"""

import matplotlib.pyplot as plt


def plot_accuracy_vs_energy(results, pareto_front):
    plt.figure(figsize=(7, 5))

    for r in results:
        plt.scatter(r["energy"], r["accuracy"], alpha=0.4)

    for r in pareto_front:
        plt.scatter(
            r["energy"],
            r["accuracy"],
            color="red",
            edgecolors="black",
            s=80,
            label="Pareto optimal",
        )

    plt.xlabel("Energy (Wh)")
    plt.ylabel("Accuracy")
    plt.title("Accuracy vs Energy (Green Pareto Frontier)")
    plt.grid(True)
    plt.show()
