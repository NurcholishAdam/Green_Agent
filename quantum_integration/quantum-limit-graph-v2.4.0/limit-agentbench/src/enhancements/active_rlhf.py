# src/enhancements/active_rlhf.py
"""
Active RLHF Querying with Asynchronous Human Feedback.

Monitors student confidence and queries humans for feedback when uncertainty is high.
Integrates with HumanReviewPortal and PolicyFeedback. Supports timeout-based fallback.

Usage:
    active = ActiveRLHF(portal, student, uncertainty_threshold=0.4, timeout=10.0)
    if active.should_query(state):
        feedback = await active.request_feedback(task, state, decision)
        active.update_student(state, feedback)
"""

import asyncio
import logging
from typing import Any, Callable, Dict, Optional

import numpy as np

logger = logging.getLogger(__name__)


class ActiveRLHF:
    """
    Active learning from human feedback.

    Attributes:
        portal: HumanReviewPortal instance for submitting review requests.
        student: Model that can predict action probabilities and be updated.
        uncertainty_threshold: Normalized entropy above which human feedback is requested.
        timeout: Seconds to wait for human feedback before falling back to autonomous action.
        reward_approved: Reward given when human approves the agent's decision.
        reward_rejected: Reward (penalty) given when human rejects the decision.
        feature_extractor: Optional callable that converts state to a feature vector.
    """

    def __init__(
        self,
        portal,
        student,
        uncertainty_threshold: float = 0.4,
        timeout: float = 30.0,
        reward_approved: float = 1.0,
        reward_rejected: float = -0.2,
        feature_extractor: Optional[Callable[[Any], np.ndarray]] = None,
    ):
        """
        Initialize the ActiveRLHF module.

        Args:
            portal: HumanReviewPortal instance.
            student: Student model with `predict_proba(features)` method.
            uncertainty_threshold: Normalized entropy threshold (0.0 to 1.0).
            timeout: Maximum time to wait for human feedback (seconds).
            reward_approved: Reward for an approved decision.
            reward_rejected: Reward for a rejected decision.
            feature_extractor: Function that extracts features from a state.
                               If None, uses `state.to_feature_vector()`.
        """
        self.portal = portal
        self.student = student
        self.threshold = uncertainty_threshold
        self.timeout = timeout
        self.reward_approved = reward_approved
        self.reward_rejected = reward_rejected
        self.feature_extractor = feature_extractor or (lambda s: s.to_feature_vector())

    def _get_features(self, state) -> np.ndarray:
        """Extract feature vector from state."""
        return np.asarray(self.feature_extractor(state), dtype=np.float32)

    def should_query(self, state) -> bool:
        """
        Decide whether to query a human based on the entropy of the student's
        action probability distribution.

        Args:
            state: Environment state.

        Returns:
            True if normalized entropy > threshold, False otherwise.
        """
        features = self._get_features(state)
        probs = self.student.predict_proba(features)
        # Ensure probabilities are valid and non‑zero for log computation.
        probs = np.clip(probs, 1e-9, 1.0)
        entropy = -np.sum(probs * np.log(probs))
        max_entropy = np.log(len(probs))
        normalized_entropy = entropy / max_entropy if max_entropy > 0 else 0.0
        return normalized_entropy > self.threshold

    async def request_feedback(self, task: Dict[str, Any], state, decision: int) -> Dict[str, Any]:
        """
        Submit a review request to the HumanReviewPortal and wait for a response.
        If no human responds within `self.timeout`, fall back to autonomous action.

        Args:
            task: Task dictionary containing at least 'task_id'.
            state: Current state (for feature extraction).
            decision: The agent's chosen action (integer).

        Returns:
            Dictionary with feedback from human or fallback:
                - 'request_id': ID from portal.
                - 'status': 'approved', 'rejected', or 'timeout'.
                - 'autonomous': True if fallback occurred, False otherwise.
        """
        features = self._get_features(state)
        request_id = self.portal.submit_for_review(
            task_id=task.get('task_id'),
            agent_output={"decision": decision, "state": features.tolist()},
            escalation_reasons=["high_uncertainty"],
            criticality_level="medium",
        )
        logger.info(f"Submitted review request {request_id} for task {task.get('task_id')}")

        # Wait for human feedback with timeout
        try:
            feedback = await asyncio.wait_for(
                self._wait_for_human(request_id),
                timeout=self.timeout,
            )
            logger.info(f"Received feedback for request {request_id}: {feedback}")
            return feedback
        except asyncio.TimeoutError:
            logger.warning(f"Timeout waiting for human feedback on request {request_id}. Falling back to autonomous.")
            # In a real system, you might proceed with the original decision,
            # possibly with reduced confidence.
            return {
                "request_id": request_id,
                "status": "timeout",
                "autonomous": True,
                "decision": decision,
            }

    async def _wait_for_human(self, request_id: str) -> Dict[str, Any]:
        """
        Poll the portal for a human response. This is a placeholder implementation;
        the actual portal should provide an async mechanism (e.g., callback or event).
        """
        # In a real system, the portal would provide an async method like
        # `await portal.get_feedback(request_id)` or a future.
        # Here we simulate a simple polling loop for demonstration.
        while True:
            # Check if feedback is available (replace with actual portal API)
            feedback = self.portal.get_feedback_if_available(request_id)
            if feedback is not None:
                return feedback
            await asyncio.sleep(0.5)  # Poll every 500 ms

    def update_student(self, state, feedback: Dict[str, Any]):
        """
        Update the student model using human (or fallback) feedback.

        Args:
            state: Environment state at decision time.
            feedback: Feedback dictionary returned by `request_feedback`.
        """
        features = self._get_features(state)
        probs = self.student.predict_proba(features)
        action = int(np.argmax(probs))  # The action that was actually taken

        # Determine reward based on feedback status
        status = feedback.get("status")
        if status == "approved":
            reward = self.reward_approved
        elif status == "rejected":
            reward = self.reward_rejected
        else:  # timeout or other
            # No human feedback; use a neutral reward or continue with original decision.
            reward = 0.0
            logger.info("No human feedback (timeout); no reward applied.")
            return

        # Update student with reward
        if hasattr(self.student, "update_with_feedback"):
            self.student.update_with_feedback(features, action, reward)
        elif hasattr(self.student, "update_from_feedback"):
            # Fallback to older method if present
            self.student.update_from_feedback(features, action, reward)
        else:
            # Generic fallback: use a method that assumes the student can be updated
            # via a simple interface like `partial_fit` or `update`.
            logger.warning(
                "Student does not implement 'update_with_feedback' or 'update_from_feedback'. "
                "Attempting to call 'update' method if available."
            )
            if hasattr(self.student, "update"):
                self.student.update(features, action, reward)
            else:
                raise NotImplementedError(
                    "Student must implement either 'update_with_feedback', "
                    "'update_from_feedback', or 'update'."
                )
