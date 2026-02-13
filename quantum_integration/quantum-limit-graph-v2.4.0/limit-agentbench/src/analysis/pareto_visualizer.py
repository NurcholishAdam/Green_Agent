import matplotlib.pyplot as plt

class ParetoVisualizer:
    """
    Visualizes Pareto frontier.
    """

    def plot(self, data):
        energy = [d["energy"] for d in data]
        latency = [d["latency"] for d in data]

        plt.scatter(energy, latency)
        plt.xlabel("Energy")
        plt.ylabel("Latency")
        plt.title("Pareto Frontier")
        plt.show()
