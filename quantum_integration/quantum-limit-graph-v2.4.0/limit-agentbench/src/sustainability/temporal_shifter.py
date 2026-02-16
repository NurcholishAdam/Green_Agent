class TemporalShifter:

    def should_delay(self, predicted_carbon, threshold):
        return predicted_carbon > threshold
