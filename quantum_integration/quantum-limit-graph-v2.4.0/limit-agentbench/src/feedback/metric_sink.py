# src/feedback/metric_sink.py

class MetricSink:
    """
    Abstract sink for emitting metrics.
    """
    def emit(self, metrics: dict):
        raise NotImplementedError


class StdoutSink(MetricSink):
    """
    Safe default sink (AgentBeats-compatible).
    """
    def emit(self, metrics: dict):
        print("[METRICS]", metrics)
