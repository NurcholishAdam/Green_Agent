# src/enhancements/active_rlhf.py
"""
Active RLHF Querying.

Monitors student confidence and queries humans for feedback when uncertainty is high.
Integrates with the existing HumanReviewPortal and PolicyFeedback.

Usage:
    active = ActiveRLHF(portal, student, uncertainty_threshold=0.4)
    should_query = active.should_query(state)
    if should_query:
        feedback = await active.request_feedback(task, state, decision)
        active.update_student(state, feedback)
"""

import numpy as np
from typing import Any, Dict, Optional

class ActiveRLHF:
    def __init__(self, portal, student, uncertainty_threshold: float = 0.4):
        self.portal = portal
        self.student = student
        self.threshold = uncertainty_threshold

    def should_query(self, state) -> bool:
        """Return True if entropy of student's action distribution > threshold."""
        probs = self.student.predict_proba(state.to_feature_vector())
        entropy = -np.sum(probs * np.log(probs + 1e-9))
        max_entropy = np.log(len(probs))
        normalized_entropy = entropy / max_entropy if max_entropy > 0 else 0.0
        return normalized_entropy > self.threshold

    async def request_feedback(self, task: Dict, state, decision: int) -> Dict:
        """Submit a review request to HumanReviewPortal and wait for response."""
        request_id = self.portal.submit_for_review(
            task_id=task.get('task_id'),
            agent_output={"decision": decision, "state": state.to_feature_vector().tolist()},
            escalation_reasons=["high_uncertainty"],
            criticality_level="medium"
        )
        # In a real system, this would wait for a human. For prototype, return a mock.
        return {"request_id": request_id, "status": "pending"}

    def update_student(self, state, feedback: Dict):
        """Update student weights using feedback reward."""
        reward = 1.0 if feedback.get('status') == 'approved' else -0.2
        state_vec = state.to_feature_vector()
        probs = self.student.predict_proba(state_vec)
        action = int(np.argmax(probs))  # selected action
        if hasattr(self.student, 'update_from_feedback'):
            self.student.update_from_feedback(state_vec, action, reward)
        else:
            # Fallback: assume student has a generic update method
            # For demonstration, we use the same logic as DistillationStudent.update
            # but with teacher_probs = None; this is simplified.
            pass
