# sustainability/eco_mode_controller.py

import logging

logger = logging.getLogger(__name__)


class EcoModeController:
    """
    Controls adaptive throttling based on carbon intensity.
    """

    def __init__(self, policy_engine, dirty_threshold: float = 400.0):
        self.policy_engine = policy_engine
        self.dirty_threshold = dirty_threshold
        self.eco_mode_enabled = False

    def update(self, carbon_intensity: float):
        if carbon_intensity > self.dirty_threshold:
            if not self.eco_mode_enabled:
                logger.warning("Entering ECO MODE due to high carbon intensity.")
                self._enable_eco_mode()
        else:
            if self.eco_mode_enabled:
                logger.info("Exiting ECO MODE.")
                self._disable_eco_mode()

    def _enable_eco_mode(self):
        self.eco_mode_enabled = True
        self.policy_engine.set_pruning_aggressiveness(0.8)
        self.policy_engine.set_token_limit_ratio(0.6)

    def _disable_eco_mode(self):
        self.eco_mode_enabled = False
        self.policy_engine.reset_defaults()
