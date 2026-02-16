import os
import time
import traceback

import torch

from core.config import PPOConfig, SystemConfig
from rl.ppo_agent import PPOAgent
from rl.q_memory import QMemory
from rl.reward_normalizer import RewardNormalizer
from policy.policy_engine import PolicyEngine
from distributed.agent_registry import AgentRegistry
from metrics.schema import create_episode_record
from system.persistence_manager import PersistenceManager
from system.health_monitor import HealthMonitor


CHECKPOINT_DIR = "checkpoints"
MODEL_PATH = os.path.join(CHECKPOINT_DIR, "ppo_model.pt")
QTABLE_PATH = os.path.join(CHECKPOINT_DIR, "q_table.json")
STATE_PATH = os.path.join(CHECKPOINT_DIR, "runtime_state.json")


def ensure_dirs():
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)


def save_runtime_state(state):
    PersistenceManager.atomic_json_save(state, STATE_PATH)


def load_runtime_state():
    if os.path.exists(STATE_PATH):
        import json
        with open(STATE_PATH) as f:
            return json.load(f)
    return {"episode": 0}


def run():

    ensure_dirs()

    # ---- CONFIG ----
    ppo_config = PPOConfig()
    system_config = SystemConfig()

    # ---- COMPONENTS ----
    agent = PPOAgent(ppo_config)
    q_memory = QMemory(QTABLE_PATH)
    reward_normalizer = RewardNormalizer()
    policy_engine = PolicyEngine(
        energy_budget=system_config.energy_budget,
        baseline_energy=50.0
    )
    registry = AgentRegistry()
    monitor = HealthMonitor()

    # ---- RECOVERY ----
    PersistenceManager.load_model(agent.model, MODEL_PATH)
    q_memory.load()

    runtime_state = load_runtime_state()
    start_episode = runtime_state["episode"]

    print(f"[INFO] Resuming from episode {start_episode}")

    episode = start_episode

    while True:

        try:
            # 1️⃣ Simulate environment interaction
            state = agent.get_state()
            action = agent.select_action(state)

            accuracy, energy, carbon = agent.execute(action)

            # 2️⃣ Sustainability reward
            sustain_reward = policy_engine.compute_sustainability_reward(
                accuracy,
                energy
            )

            total_reward = accuracy + sustain_reward

            # 3️⃣ Normalize reward
            reward_normalizer.update(total_reward)
            total_reward = reward_normalizer.normalize(total_reward)

            # 4️⃣ PPO update
            loss = agent.update(state, action, total_reward)

            # 5️⃣ Health checks
            monitor.check_loss(loss)
            monitor.check_energy(energy, system_config.energy_budget)
            monitor.check_reward(total_reward)

            # 6️⃣ QMemory update
            q_memory.update(state, action, total_reward)

            # 7️⃣ Telemetry record
            record = create_episode_record(
                episode,
                accuracy,
                energy,
                carbon,
                total_reward,
                provenance_energy="measured",
                provenance_carbon="forecast"
            )

            # 8️⃣ Distributed registry update
            registry.register(agent.agent_id, record)

            # 9️⃣ Periodic save
            if episode % system_config.save_interval == 0:
                PersistenceManager.atomic_model_save(agent.model, MODEL_PATH)
                q_memory.save()
                save_runtime_state({"episode": episode})

            # 🔟 Periodic coordination
            if episode % system_config.coordinator_sync_interval == 0:
                global_metrics = registry.aggregate()
                print(f"[INFO] Coordinator sees {len(global_metrics)} agents")

            episode += 1

        except Exception as e:
            print("[CRASH DETECTED]")
            traceback.print_exc()

            # Save crash snapshot
            PersistenceManager.atomic_model_save(agent.model, MODEL_PATH)
            q_memory.save()
            save_runtime_state({"episode": episode})

            print("[INFO] Recovery checkpoint saved. Restarting in 3 seconds...")
            time.sleep(3)
