# MOPD (Multi-Teacher On-Policy Distillation) Integration

This document describes the MOPD reporting and integration points added to the Green_Agent enhancements module.

Overview
- DistillationOrchestrator: can run in-process with AdaptiveCostFunction or fall back to HTTP reporting.
- AdaptiveCostFunction: now exposes a REST endpoint POST /mopd/record which accepts per-teacher distillation reports and forwards them to the internal feedback pipeline.
- FeedbackCollector: forwards teacher_id and distillation_loss (if present) into AdaptiveCostFunction.record_feedback so both inference-time and training-time signals use the same persistence and weight-update pathway.

In-process reporting (recommended)
- Instantiate AdaptiveCostFunction and inject dependencies (db_manager, registry) before running distillation.
  Example:

  from quantum_integration.quantum_limit_graph_v2_4_0.limit_agentbench.src.enhancements.adaptive_cost_function import AdaptiveCostFunction, AsyncDatabaseManager
  from quantum_integration.quantum_limit_graph_v2_4_0.limit_agentbench.src.enhancements.distillation_orchestrator import DistillationOrchestrator

  adaptive_cfg = { 'learning_rate': 0.01, 'db_backend': 'sqlite', 'enable_mopd': True }
  adaptive = AdaptiveCostFunction(adaptive_cfg)
  dbm = AsyncDatabaseManager(adaptive._config_obj)
  await dbm.init()
  # supply a real ExpertRegistry implementation as `registry`
  adaptive.inject_dependencies(dbm, registry)

  distil_cfg = { 'num_epochs': 3, 'batch_size': 32, 'expert_id': 'distill_expert', 'node_id': 'node-1' }
  orchestrator = DistillationOrchestrator(student_model, teachers_dict, distil_cfg, adaptive_function_instance=adaptive)
  await orchestrator.distill(train_dataloader)

- The DistillationOrchestrator will call adaptive.record_feedback(context, metrics, teacher_id=..., distillation_loss=...) after each epoch for the teachers used.
- The AdaptiveCostFunction.record_feedback will persist the record and enqueue it for mini-batch updates that may update teacher weights.

HTTP reporting (fallback)
- If you run the distillation job as a separate process, configure the DistillationOrchestrator with:
  - adaptive_api_url: e.g. "http://adaptive-host:8000"
  - adaptive_api_token: optional Bearer token for authentication

- DistillationOrchestrator will POST to {adaptive_api_url}/mopd/record with JSON payloads of the form:
  {
    "context": {"request_id": "<uuid>", "expert_id": "<id>", "node_id": "<id>"},
    "metrics": {...},
    "teacher_id": "<teacher-id>",
    "distillation_loss": 0.123,
    "epoch": 1
  }

- The adaptive service will validate and call AdaptiveCostFunction.record_feedback internally.

Security & deployment notes
- The current FastAPI JWT verification implementation in adaptive_cost_function.py is a placeholder and accepts any token as admin. Replace verify_jwt(...) with proper JWT verification for production.
- Ensure AdaptiveCostFunction.inject_dependencies(...) is called before relying on in-process reporting so database/registry are available.
- Consider limiting reporting frequency (e.g., top-k teachers, once per N epochs) if you have many teachers.

Testing
- A unit test was added verifying in-process reporting (tests/test_mopd_integration.py).
- A new HTTP endpoint test is included (tests/test_mopd_http.py) which exercises POST /mopd/record using FastAPI's test client (httpx AsyncClient) and a mocked adaptive_function.

Files changed/added
- enhancements/distillation_orchestrator.py (in-process + HTTP fallback)
- enhancements/feedback_collector.py (forward teacher info)
- enhancements/adaptive_cost_function.py (POST /mopd/record endpoint)
- tests/test_mopd_integration.py (in-process integration test)
- tests/test_mopd_http.py (HTTP endpoint test)

If you'd like, I can also:
- Add a short CI job to run these tests in GitHub Actions.
- Extend the payload to include per-batch energy/carbon telemetry for finer-grained teacher weighting.

