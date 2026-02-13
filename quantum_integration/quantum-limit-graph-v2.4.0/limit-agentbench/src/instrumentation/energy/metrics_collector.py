class MetricsCollector:
    """
    Aggregates all energy and runtime meters.
    """

    def __init__(self, meters):
        self.meters = meters

    def start_all(self):
        for m in self.meters:
            m.start()

    def stop_all(self):
        results = {}
        for m in self.meters:
            results[m.__class__.__name__] = m.stop()
        return results
