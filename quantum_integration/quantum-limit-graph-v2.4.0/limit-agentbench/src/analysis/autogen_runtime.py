"""
AutoGen runtime adapter for Green Agent.

Extracts real message graph depth when AutoGen is available.
"""

from typing import Dict, Any
from src.analysis.runtime_adapter import AgentRuntime


class AutoGenRuntime(AgentRuntime):
    """
    Executes queries using AutoGen and extracts conversation graphs.
    """

    def init(self, config: Dict[str, Any]):
        self.enabled = False
        self.messages = []

        try:
            import autogen  # noqa

            self.agents = config.get("agents")
            if not self.agents:
                raise ValueError("AutoGen agents not provided")

            self.enabled = True

        except Exception as e:
            self.init_error = str(e)
            self.enabled = False

    def _record_message(self, msg):
        self.messages.append({
            "from": getattr(msg, "sender", "unknown"),
            "to": getattr(msg, "recipient", "unknown"),
            "content": getattr(msg, "content", ""),
        })

    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        if not self.enabled:
            return {
                "output": None,
                "tool_calls": 0,
                "conversation_depth": 0,
                "error": "AutoGen unavailable",
            }

        for msg in self.agents.run(query.get("input", "")):
            self._record_message(msg)

        nodes = set()
        for m in self.messages:
            nodes.add(m["from"])
            nodes.add(m["to"])

        return {
            "output": None,
            "tool_calls": 0,
            "conversation_depth": len(self.messages),
            "agent_nodes": len(nodes),
        }

    def finalize(self):
        self.messages.clear()
