@@
                 # Feed to cost function with retry and circuit breaker
-                @retry_decorator(attempts=self.config.max_retry_attempts)
-                async def feed():
-                    await self.cost_function.record_feedback(context, metrics)
+                @retry_decorator(attempts=self.config.max_retry_attempts)
+                async def feed():
+                    # Forward teacher_id and distillation_loss if present in either metrics or context
+                    teacher_id = metrics.get('teacher_id') or context.get('teacher_id')
+                    distill_loss = metrics.get('distillation_loss') or context.get('distillation_loss')
+                    await self.cost_function.record_feedback(context, metrics, teacher_id=teacher_id, distillation_loss=distill_loss)
@@
                 await self._circuit_breaker.call(feed)
