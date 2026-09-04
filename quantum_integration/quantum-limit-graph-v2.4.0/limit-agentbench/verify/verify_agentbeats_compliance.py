#!/usr/bin/env python3
"""
AgentBeats Compliance Verification Script for Green_Agent (Enhanced)
Run this in your repository root to verify AgentBeats compliance
and optionally check for advanced enhancement modules (LIMIT Graph, MODP, RLHF,
Multi‑Teacher On‑Policy Distillation, Bio‑inspired, MoE expert gating).
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple

class AgentBeatsVerifier:
    def __init__(self):
        self.repo_root = Path.cwd()
        self.results = {
            "a2a_compliance": {"score": 0, "max": 25, "checks": []},
            "docker_independence": {"score": 0, "max": 25, "checks": []},
            "robust_scoring": {"score": 0, "max": 25, "checks": []},
            "rlhf_feedback": {"score": 0, "max": 25, "checks": []},
        }
        # New optional category for advanced enhancements
        self.enhancement_results = {"score": 0, "max": 25, "checks": []}

    def verify_all(self):
        """Run all verification checks including optional enhancements"""
        print("🔍 AgentBeats Compliance Verification (Enhanced)")
        print("=" * 50)

        self.verify_a2a_compliance()
        self.verify_docker_independence()
        self.verify_robust_scoring()
        self.verify_rlhf_feedback()
        self.verify_advanced_enhancements()  # new check

        self.print_report()
        self.print_enhancement_report()

    def verify_a2a_compliance(self):
        """Verify A2A protocol implementation"""
        print("\n📡 Checking A2A Compliance...")
        category = self.results["a2a_compliance"]

        # Check 1: A2A Handler exists
        a2a_handler = self.repo_root / "quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/agentbeats/a2a_handler.py"
        if a2a_handler.exists():
            category["score"] += 10
            category["checks"].append(("✅", "A2A handler exists", 10))

            # Verify it has required methods
            content = a2a_handler.read_text()
            if "async def send_task" in content:
                category["checks"].append(("✅", "  - send_task method found", 0))
            else:
                category["checks"].append(("❌", "  - send_task method MISSING", 0))

            if "async def get_result" in content:
                category["checks"].append(("✅", "  - get_result method found", 0))
            else:
                category["checks"].append(("❌", "  - get_result method MISSING", 0))
        else:
            category["checks"].append(("❌", "A2A handler MISSING", 0))

        # Check 2: Green Agent exists
        green_agent = self.repo_root / "quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/agentbeats/green_agent.py"
        if green_agent.exists():
            category["score"] += 10
            category["checks"].append(("✅", "Green agent orchestrator exists", 10))

            content = green_agent.read_text()
            if "async def handle_assessment_request" in content:
                category["checks"].append(("✅", "  - handle_assessment_request found", 0))
            else:
                category["checks"].append(("❌", "  - handle_assessment_request MISSING", 0))
        else:
            category["checks"].append(("❌", "Green agent orchestrator MISSING", 0))

        # Check 3: FastAPI endpoints
        main_py = self.repo_root / "quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/agentbeats/main.py"
        if main_py.exists():
            content = main_py.read_text()

            has_task_endpoint = '@app.post("/a2a/task")' in content
            has_health_endpoint = '@app.get("/health")' in content

            if has_task_endpoint and has_health_endpoint:
                category["score"] += 5
                category["checks"].append(("✅", "FastAPI A2A endpoints exist", 5))
            else:
                if not has_task_endpoint:
                    category["checks"].append(("❌", "  - POST /a2a/task MISSING", 0))
                if not has_health_endpoint:
                    category["checks"].append(("❌", "  - GET /health MISSING", 0))
        else:
            category["checks"].append(("❌", "FastAPI main.py MISSING", 0))

    def verify_docker_independence(self):
        """Verify Docker configuration"""
        print("\n🐳 Checking Docker Independence...")
        category = self.results["docker_independence"]

        dockerfile = self.repo_root / "Dockerfile"
        if dockerfile.exists():
            category["score"] += 10
            category["checks"].append(("✅", "Dockerfile exists", 10))

            content = dockerfile.read_text()

            if "EXPOSE 8000" in content:
                category["checks"].append(("✅", "  - Exposes port 8000", 0))
            else:
                category["checks"].append(("❌", "  - Does NOT expose port 8000", 0))

            if "HEALTHCHECK" in content:
                category["checks"].append(("✅", "  - Has healthcheck", 0))
            else:
                category["checks"].append(("⚠️", "  - Missing healthcheck (recommended)", 0))

            if "CMD" in content or "ENTRYPOINT" in content:
                category["checks"].append(("✅", "  - Has startup command", 0))
            else:
                category["checks"].append(("❌", "  - MISSING startup command", 0))
        else:
            category["checks"].append(("❌", "Dockerfile MISSING", 0))

        compose = self.repo_root / "docker-compose.yml"
        if compose.exists():
            category["score"] += 5
            category["checks"].append(("✅", "docker-compose.yml exists", 5))
        else:
            category["checks"].append(("⚠️", "docker-compose.yml missing (recommended)", 0))

        print("  Testing Docker build...")
        try:
            result = subprocess.run(
                ["docker", "build", "-t", "green-agent:test", "."],
                cwd=self.repo_root,
                capture_output=True,
                timeout=300
            )
            if result.returncode == 0:
                category["score"] += 10
                category["checks"].append(("✅", "Docker builds successfully", 10))
            else:
                category["checks"].append(("❌", "Docker build FAILED", 0))
                category["checks"].append(("", f"  Error: {result.stderr.decode()[:100]}", 0))
        except subprocess.TimeoutExpired:
            category["checks"].append(("❌", "Docker build TIMEOUT (>5min)", 0))
        except FileNotFoundError:
            category["checks"].append(("⚠️", "Docker not installed - cannot test build", 0))

    def verify_robust_scoring(self):
        """Verify robust scoring implementation"""
        print("\n🎯 Checking Robust Scoring...")
        category = self.results["robust_scoring"]

        scorer = self.repo_root / "quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/scoring/robust_scorer.py"
        if scorer.exists():
            category["score"] += 15
            category["checks"].append(("✅", "Robust scorer exists", 15))

            content = scorer.read_text()

            if "_handle_timeout" in content:
                category["checks"].append(("✅", "  - Handles timeouts", 0))
            else:
                category["checks"].append(("❌", "  - Missing timeout handler", 0))

            if "_handle_error" in content or "_handle_oom" in content:
                category["checks"].append(("✅", "  - Handles errors/OOM", 0))
            else:
                category["checks"].append(("❌", "  - Missing error handlers", 0))

            if "partial_credit" in content:
                category["score"] += 10
                category["checks"].append(("✅", "  - Implements partial credit", 10))
            else:
                category["checks"].append(("❌", "  - Missing partial credit", 0))
        else:
            category["checks"].append(("❌", "Robust scorer MISSING", 0))

        classifier = self.repo_root / "quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/scoring/failure_classifier.py"
        if classifier.exists():
            category["checks"].append(("✅", "Failure classifier exists", 0))
        else:
            category["checks"].append(("⚠️", "Failure classifier missing (recommended)", 0))

    def verify_rlhf_feedback(self):
        """Verify RLHF feedback system"""
        print("\n🤖 Checking RLHF Feedback...")
        category = self.results["rlhf_feedback"]

        engine = self.repo_root / "quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/feedback/rlhf_engine.py"
        if engine.exists():
            category["score"] += 10
            category["checks"].append(("✅", "RLHF engine exists", 10))

            content = engine.read_text()
            if "generate_feedback" in content:
                category["checks"].append(("✅", "  - generate_feedback method found", 0))
            else:
                category["checks"].append(("❌", "  - generate_feedback MISSING", 0))
        else:
            category["checks"].append(("❌", "RLHF engine MISSING", 0))

        analyzer = self.repo_root / "quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/feedback/reasoning_analyzer.py"
        if analyzer.exists():
            category["score"] += 10
            category["checks"].append(("✅", "Reasoning analyzer exists", 10))
        else:
            category["checks"].append(("❌", "Reasoning analyzer MISSING", 0))

        suggester = self.repo_root / "quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/feedback/improvement_suggester.py"
        if suggester.exists():
            category["score"] += 5
            category["checks"].append(("✅", "Improvement suggester exists", 5))
        else:
            category["checks"].append(("⚠️", "Improvement suggester missing (recommended)", 0))

    def verify_advanced_enhancements(self):
        """
        Optional verification for advanced enhancement modules:
        LIMIT Graph, MODP, RLHF, Multi‑Teacher Distillation, Bio‑inspired, MoE.
        These are not required for AgentBeats compliance but indicate advanced
        decision-making capabilities.
        """
        print("\n🧠 Checking Advanced Enhancements (Optional, Bonus)...")
        cat = self.enhancement_results

        enhancements_dir = self.repo_root / "quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements"

        # Check for key files
        files_to_check = {
            "feedback_event.py": "Canonical MOPD event schema",
            "node_descriptor.py": "Adaptive node routing (distillation + MoE + RLHF)",
            "workload_descriptor.py": "Adaptive workload priority",
            "zero_trust_architecture.py": "Zero Trust security with distillation",
            "async_message_queue.py": "Cross‑module messaging",
        }

        for fname, desc in files_to_check.items():
            fpath = enhancements_dir / fname
            if fpath.exists():
                cat["score"] += 3
                cat["checks"].append(("✅", f"{fname} exists ({desc})", 3))
                # Basic content checks for indicators of enhancements
                content = fpath.read_text()
                if fname == "node_descriptor.py":
                    if "MoEGatingNetwork" in content and "RLHFTeacher" in content:
                        cat["score"] += 2  # extra bonus for having both MoE and RLHF
                        cat["checks"].append(("✅", "  - Contains MoE gating and RLHF teacher", 2))
                elif fname == "workload_descriptor.py":
                    if "EvolutionaryOptimizer" in content:
                        cat["score"] += 1
                        cat["checks"].append(("✅", "  - Contains evolutionary optimizer", 1))
            else:
                cat["checks"].append(("❌", f"{fname} MISSING", 0))

        # Check for graph registry (LIMIT Graph)
        graph_registry = self.repo_root / "quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/core/graph_registry.py"
        if graph_registry.exists():
            cat["score"] += 3
            cat["checks"].append(("✅", "graph_registry.py exists (LIMIT Graph)", 3))
        else:
            cat["checks"].append(("⚠️", "graph_registry.py not found (LIMIT Graph may be missing)", 0))

        # Check for policy graph / causal graph (related to LIMIT Graph)
        policy_graph = self.repo_root / "quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/core/policy_graph.py"
        if policy_graph.exists():
            cat["score"] += 2
            cat["checks"].append(("✅", "policy_graph.py exists", 2))
        else:
            cat["checks"].append(("⚠️", "policy_graph.py not found (recommended for LIMIT Graph)", 0))

    def print_report(self):
        """Print main compliance report (original categories only)"""
        print("\n" + "=" * 50)
        print("📊 COMPLIANCE REPORT (Core)")
        print("=" * 50)

        total_score = 0
        total_max = 0

        for category_name, category_data in self.results.items():
            score = category_data["score"]
            max_score = category_data["max"]
            total_score += score
            total_max += max_score

            print(f"\n{category_name.replace('_', ' ').title()}:")
            print(f"  Score: {score}/{max_score} ({score/max_score*100:.1f}%)")

            for icon, check, points in category_data["checks"]:
                if points > 0:
                    print(f"  {icon} {check} (+{points})")
                else:
                    print(f"  {icon} {check}")

        print("\n" + "=" * 50)
        print(f"TOTAL SCORE: {total_score}/{total_max} ({total_score/total_max*100:.1f}%)")
        print("=" * 50)

        percentage = total_score / total_max * 100
        if percentage >= 90:
            print("\n🟢 STATUS: PRODUCTION READY")
            print("✅ Your agent is ready for AgentBeats submission!")
            print("\nNext Steps:")
            print("1. Create 3-minute demo video")
            print("2. Register on AgentBeats platform")
            print("3. Test with baseline purple agent")
            print("4. Submit to competition")
        elif percentage >= 75:
            print("\n🟡 STATUS: NEARLY READY")
            print("⚠️ Minor fixes needed before submission")
            print("\nFocus on:")
            for cat, data in self.results.items():
                if data["score"] < data["max"]:
                    print(f"  - {cat.replace('_', ' ').title()}")
        elif percentage >= 60:
            print("\n🟠 STATUS: MAJOR WORK NEEDED")
            print("🔧 Estimated 1-2 weeks of work required")
            print("\nPriority areas:")
            sorted_cats = sorted(self.results.items(), key=lambda x: x[1]["score"]/x[1]["max"])
            for cat, data in sorted_cats[:2]:
                print(f"  - {cat.replace('_', ' ').title()} ({data['score']}/{data['max']})")
        else:
            print("\n🔴 STATUS: NOT READY")
            print("❌ Significant architecture work required")
            print("📋 Review the AgentBeats transformation roadmap")
            print("⏱️ Estimated 3-4 weeks to completion")
        print("\n")

    def print_enhancement_report(self):
        """Print optional advanced enhancements report"""
        cat = self.enhancement_results
        print("\n" + "=" * 50)
        print("📊 ADVANCED ENHANCEMENTS (Optional, Bonus)")
        print("=" * 50)
        print(f"Score: {cat['score']}/{cat['max']}")
        for icon, check, points in cat["checks"]:
            if points > 0:
                print(f"  {icon} {check} (+{points})")
            else:
                print(f"  {icon} {check}")
        print("\n" + "=" * 50)


if __name__ == "__main__":
    verifier = AgentBeatsVerifier()
    verifier.verify_all()
