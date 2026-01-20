# Add: src/agentbeats/mcp_server.py

class GreenAgentMCPServer:
    """Provides tools for purple agents via MCP"""
    
    @tool
    def get_energy_budget(self) -> dict:
        """Tool: Get remaining energy budget for task"""
        
    @tool
    def submit_carbon_report(self, emissions: float):
        """Tool: Submit carbon emissions data"""