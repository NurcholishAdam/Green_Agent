class CarbonForecast:

    def __init__(self, window_size=24):
        self.window_size = window_size
        self.history = []

    def update(self, carbon_intensity):
        self.history.append(carbon_intensity)
        if len(self.history) > self.window_size:
            self.history.pop(0)

    def predict_next(self):
        if not self.history:
            return 0.0
        return sum(self.history) / len(self.history)
