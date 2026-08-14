"""
carbon_delay_scheduler.py

Implements a carbon‑intensity‑aware delay queue.
Tasks are deferred to a cleaner time window if:
  - they are not urgent (priority != 'high'),
  - current carbon intensity exceeds a threshold,
  - a lower‑intensity window exists within max_delay_seconds.
"""
import heapq
import time
from typing import Dict, Any, Optional, List, Tuple


class CarbonDelayScheduler:
    """
    Schedules tasks to run during lower‑carbon periods.
    """
    def __init__(
        self,
        carbon_api: Any,
        max_delay_seconds: int = 3600,
        threshold_gco2_per_kwh: float = 150.0,
    ):
        """
        Args:
            carbon_api: Object with methods:
                - get_current() -> float (gCO2/kWh)
                - get_forecast(minutes: int) -> List[Tuple[float, float]]
                  returns list of (timestamp, intensity) for future minutes.
            max_delay_seconds: Maximum allowable delay.
            threshold_gco2_per_kwh: Delay if current intensity > this.
        """
        self.carbon_api = carbon_api
        self.max_delay = max_delay_seconds
        self.threshold = threshold_gco2_per_kwh
        self.queue = []  # min‑heap of (scheduled_timestamp, task)

    def submit(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Returns a dict with keys:
            - 'status': 'delayed' or 'forward'
            - 'task': the original or same task (unchanged)
            - 'delay_until': timestamp if delayed, else None
        """
        if task.get("priority") == "high":
            return {"status": "forward", "task": task, "delay_until": None}

        current_intensity = self.carbon_api.get_current()
        if current_intensity <= self.threshold:
            return {"status": "forward", "task": task, "delay_until": None}

        # Get forecast for the next max_delay seconds
        forecast_minutes = self.max_delay // 60 + 1
        forecast = self.carbon_api.get_forecast(forecast_minutes)
        if not forecast:
            # If API fails, forward immediately (fail‑open)
            return {"status": "forward", "task": task, "delay_until": None}

        now = time.time()
        # Find the first timestamp where intensity < threshold
        best_time = None
        for ts, intensity in forecast:
            if intensity < self.threshold:
                best_time = ts
                break

        if best_time is None or (best_time - now) > self.max_delay:
            return {"status": "forward", "task": task, "delay_until": None}

        # Schedule the task
        heapq.heappush(self.queue, (best_time, task))
        return {"status": "delayed", "task": task, "delay_until": best_time}

    def tick(self) -> List[Dict[str, Any]]:
        """
        Called periodically (e.g., every minute) to release due tasks.
        Returns a list of tasks that are ready to be forwarded.
        """
        now = time.time()
        released = []
        while self.queue and self.queue[0][0] <= now:
            _, task = heapq.heappop(self.queue)
            released.append(task)
        return released
