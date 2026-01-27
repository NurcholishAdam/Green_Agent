# src/feedback/metric_sink.py

class MetricSink:
    def emit(self, metrics: dict):
        pass


class StdoutSink(MetricSink):
    def emit(self, metrics: dict):
        print("[METRIC]", metrics)
