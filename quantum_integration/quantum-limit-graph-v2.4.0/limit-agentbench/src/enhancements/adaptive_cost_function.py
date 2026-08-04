@@
-from fastapi import FastAPI, Depends, HTTPException, status, Request, BackgroundTasks
-from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
+from fastapi import FastAPI, Depends, HTTPException, status, Request, BackgroundTasks
+from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
+import os
+import jwt
+from jwt import PyJWTError
@@
-security = HTTPBearer()
-async def verify_jwt(token: str) -> Dict:
-    # In production, verify JWT properly and extract roles.
-    # For demo, we accept any token and assign role based on presence.
-    return {"sub": "admin", "role": "admin"}
-
-async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
-    return await verify_jwt(credentials.credentials)
-
-async def require_admin(user: Dict = Depends(get_current_user)):
-    if user.get("role") != "admin":
-        raise HTTPException(status_code=403, detail="Admin role required")
-    return user
+security = HTTPBearer()
+
+# JWT auth helper using HS256 secret (or configure JWKS for RS256 in production)
+AUTH_SECRET = os.environ.get("ADAPTIVE_API_HS256_SECRET", "dev-secret-change-me")
+AUTH_ALGORITHM = os.environ.get("ADAPTIVE_API_ALGO", "HS256")
+
+async def verify_jwt(token: str) -> Dict:
+    if not token:
+        raise HTTPException(status_code=401, detail="Missing token")
+    try:
+        payload = jwt.decode(token, AUTH_SECRET, algorithms=[AUTH_ALGORITHM])
+    except PyJWTError:
+        raise HTTPException(status_code=401, detail="Invalid token")
+    # Support roles claim or scope string
+    roles = payload.get("roles") or payload.get("scope") or []
+    if isinstance(roles, str):
+        roles = roles.split()
+    return {"sub": payload.get("sub"), "roles": roles, "claims": payload}
+
+async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
+    return await verify_jwt(credentials.credentials)
+
+async def require_admin(user: Dict = Depends(get_current_user)):
+    roles = user.get("roles", []) or []
+    if "admin" not in roles:
+        raise HTTPException(status_code=403, detail="Admin role required")
+    return user
+
+async def require_trainer(user: Dict = Depends(get_current_user)):
+    roles = user.get("roles", []) or []
+    if "trainer" not in roles and "admin" not in roles:
+        raise HTTPException(status_code=403, detail="Trainer role required")
+    return user
@@
-    async def _persist_feedback_inner(self, context: Dict, actual: Dict, pred: float, actual_cost: float,
-                                       teacher_id: Optional[str], distillation_loss: Optional[float]):
-        async with self.db_manager.get_session() as session:
-            await session.execute(
-                text("""
-                    INSERT INTO feedback_records
-                    (request_id, expert_id, node_id, predicted_cost, actual_cost,
-                     energy_joules, carbon_kg, helium_units, latency_ms, accuracy,
-                     weights_snapshot, teacher_id, distillation_loss)
-                    VALUES (:request_id, :expert_id, :node_id, :predicted_cost, :actual_cost,
-                     :energy_joules, :carbon_kg, :helium_units, :latency_ms, :accuracy,
-                     :weights_snapshot, :teacher_id, :distillation_loss)
-                """),
-                {
-                    'request_id': context.get('request_id'),
-                    'expert_id': context.get('expert_id'),
-                    'node_id': context.get('node_id'),
-                    'predicted_cost': pred,
-                    'actual_cost': actual_cost,
-                    'energy_joules': actual.get('energy_joules', 0),
-                    'carbon_kg': actual.get('carbon_kg', 0),
-                    'helium_units': actual.get('helium_units', 0),
-                    'latency_ms': actual.get('latency_ms', 0),
-                    'accuracy': actual.get('accuracy', 0),
-                    'weights_snapshot': json.dumps(self.weights),
-                    'teacher_id': teacher_id,
-                    'distillation_loss': distillation_loss
-                }
-            )
-            await session.commit()
+    async def _persist_feedback_inner(self, context: Dict, actual: Dict, pred: float, actual_cost: float,
+                                       teacher_id: Optional[str], distillation_loss: Optional[float]):
+        async with self.db_manager.get_session() as session:
+            # Persist extra metrics inside weights_snapshot JSON so schema need not change
+            weights_snapshot_json = {'weights': self.weights, 'extra_metrics': actual}
+            await session.execute(
+                text("""
+                    INSERT INTO feedback_records
+                    (request_id, expert_id, node_id, predicted_cost, actual_cost,
+                     energy_joules, carbon_kg, helium_units, latency_ms, accuracy,
+                     weights_snapshot, teacher_id, distillation_loss)
+                    VALUES (:request_id, :expert_id, :node_id, :predicted_cost, :actual_cost,
+                     :energy_joules, :carbon_kg, :helium_units, :latency_ms, :accuracy,
+                     :weights_snapshot, :teacher_id, :distillation_loss)
+                """),
+                {
+                    'request_id': context.get('request_id'),
+                    'expert_id': context.get('expert_id'),
+                    'node_id': context.get('node_id'),
+                    'predicted_cost': pred,
+                    'actual_cost': actual_cost,
+                    'energy_joules': actual.get('energy_joules', 0),
+                    'carbon_kg': actual.get('carbon_kg', 0),
+                    'helium_units': actual.get('helium_units', 0),
+                    'latency_ms': actual.get('latency_ms', 0),
+                    'accuracy': actual.get('accuracy', 0),
+                    'weights_snapshot': json.dumps(weights_snapshot_json),
+                    'teacher_id': teacher_id,
+                    'distillation_loss': distillation_loss
+                }
+            )
+            await session.commit()
@@
     async def record_feedback(
@@
-        # Persist feedback record (with retry)
-        await self._persist_feedback(context, actual_metrics, predicted_cost, actual_cost,
-                                     teacher_id, distillation_loss)
+        # Persist feedback record (with retry)
+        await self._persist_feedback(context, actual_metrics, predicted_cost, actual_cost,
+                                     teacher_id, distillation_loss)
+
+        # Prometheus: set distillation loss gauge if provided
+        try:
+            if distillation_loss is not None:
+                DISTILLATION_LOSS.set(float(distillation_loss)
+                                      if isinstance(DISTILLATION_LOSS, Gauge) else distillation_loss)
+        except Exception:
+            # ignore metric failures
+            pass
