"""
Feedback Collector enhancements: pass through teacher_id and distillation_loss when present
"""

# (This file is part of the repo; we will only modify the _process_batch internal feed wrapper to forward teacher info)

# Locate the existing feed inner function and modify its call to cost_function.record_feedback
# Patch applied by updating the function where feed() is defined in _process_batch.
