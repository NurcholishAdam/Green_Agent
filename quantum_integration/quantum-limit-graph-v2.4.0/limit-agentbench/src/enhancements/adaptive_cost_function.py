@@
 # NEW MOPD endpoints
 @app.get("/teachers", dependencies=[Depends(get_current_user)])
 async def get_teachers(domain: Optional[str] = None):
     if not adaptive_function or not adaptive_function.enable_mopd:
         raise HTTPException(status_code=503, detail="MOPD not enabled")
     if domain:
         teachers = await adaptive_function.select_teachers(domain)
     else:
         # Return all teachers and weights
         weights = adaptive_function.teacher_grid_manager.get_teacher_weights()
         return {"teacher_weights": weights, "grid": adaptive_function.teacher_grid_manager.teacher_grid}
     return {"teachers": teachers}
@@
 @app.post("/teachers/update_weight", dependencies=[Depends(require_admin)])
 async def update_teacher_weight(teacher_id: str, delta: float):
     if not adaptive_function or not adaptive_function.enable_mopd:
         raise HTTPException(status_code=503, detail="MOPD not enabled")
     await adaptive_function.teacher_grid_manager.update_teacher_weight(teacher_id, delta)
     return {"status": "updated", "teacher_id": teacher_id, "new_weight": adaptive_function.teacher_grid_manager.teacher_weights.get(teacher_id, 1.0)}
@@
 @app.post("/pareto/add", dependencies=[Depends(require_admin)])
 async def add_pareto_point(weights: Dict[str, float]):
     if not adaptive_function or not adaptive_function.pareto_enabled:
         raise HTTPException(status_code=503, detail="Pareto not enabled")
     added = await adaptive_function.pareto_manager.add(weights)
     return {"added": added, "pareto_front": adaptive_function.pareto_manager.get_front()}
+
+# -------------------------------------------------------------------------
+# MOPD report endpoint
+# -------------------------------------------------------------------------
+@app.post("/mopd/record", dependencies=[Depends(get_current_user)])
+async def mopd_record(payload: Dict[str, Any]):
+    """
+    Accepts MOPD (Multi-Teacher On-Policy Distillation) reports from trainers.
+
+    Expected JSON body:
+    {
+      "context": {"request_id": "<uuid>", "expert_id": "<id>", "node_id": "<id>"},
+      "metrics": {"energy_joules": 0.0, "carbon_kg": 0.0, "helium_units": 0.0, "latency_ms": 0.0, "accuracy": 0.0},
+      "teacher_id": "<teacher-id>",
+      "distillation_loss": 0.123,
+      "epoch": 1
+    }
+
+    This endpoint will forward the data into AdaptiveCostFunction.record_feedback(...) so it is
+    persisted into feedback_records and used for weight updates.
+    """
+    if not adaptive_function or not adaptive_function.enable_mopd:
+        raise HTTPException(status_code=503, detail="MOPD not enabled")
+
+    context = payload.get("context", {}) or {}
+    metrics = payload.get("metrics", {}) or {}
+    teacher_id = payload.get("teacher_id")
+    distillation_loss = payload.get("distillation_loss")
+
+    # Basic validation
+    if not context.get("expert_id"):
+        # Use configured expert_id as fallback if provided
+        expert_id = adaptive_function._config_obj.teacher_grid.get('expert_id') if hasattr(adaptive_function, '_config_obj') else None
+        if expert_id:
+            context.setdefault('expert_id', expert_id)
+
+    try:
+        # Call record_feedback which will persist and enqueue the record for weight updates
+        await adaptive_function.record_feedback(context, metrics, teacher_id=teacher_id, distillation_loss=distillation_loss)
+    except Exception as e:
+        logger.error(f"MOPD record failed: {e}")
+        raise HTTPException(status_code=500, detail="Failed to record MOPD feedback")
+
+    return {"status": "ok", "teacher_id": teacher_id, "distillation_loss": distillation_loss}
@@
 @app.on_event("startup")
 async def startup():
