import subprocess

class PerfCounter:
    """
    Hardware performance counters using Linux perf.
    """

    def collect(self):
        result = subprocess.run(
            ["perf", "stat", "-e", "cycles,instructions", "sleep", "0.1"],
            capture_output=True,
            text=True
        )
        return {
            "raw_output": result.stderr,
            "provenance": "measured"
        }
