"""
Specialized 2D Pareto Plots for Green_Agent

Implements three critical policy-oriented visualizations:
1. Accuracy vs Carbon - Sustainability reviewers' view
2. Latency vs Energy - Systems engineers' view  
3. Carbon vs Energy (Pure Green) - Pure environmental efficiency

Why multiple 2D plots instead of one 7D plot:
- Humans cannot reason in 7D, but policies are 2D
- Each plot answers a different policy question
- Projections reveal different "faces" of the Pareto frontier
"""

from typing import List, Optional, Dict, Tuple
import numpy as np
import logging

# Import visualization libraries
try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    logging.warning("Plotly not available. Install with: pip install plotly")

try:
    import matplotlib.pyplot as plt
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    logging.warning("Matplotlib not available. Install with: pip install matplotlib")

logger = logging.getLogger(__name__)


class ParetoPlotter:
    """
    Specialized 2D Pareto visualizations
    
    Each plot serves a specific policy/deployment question:
    
    1. Accuracy vs Carbon
       Question: "What performance am I paying per unit environmental cost?"
       Users: Sustainability reviewers, policy makers, ESG reporters
    
    2. Latency vs Energy
       Question: "Are fast agents inherently wasteful?"
       Users: Systems engineers, edge deployment teams
    
    3. Carbon vs Energy (Pure Green)
       Question: "Which agents are environmentally efficient independent of performance?"
       Users: Green AI researchers, carbon-budget planners
    """
    
    def __init__(self, backend: str = 'plotly'):
        """
        Initialize plotter
        
        Args:
            backend: 'plotly' (interactive) or 'matplotlib' (static)
        """
        self.backend = backend
        
        if backend == 'plotly' and not PLOTLY_AVAILABLE:
            raise ImportError("Plotly not installed. Run: pip install plotly")
        if backend == 'matplotlib' and not MATPLOTLIB_AVAILABLE:
            raise ImportError("Matplotlib not installed. Run: pip install matplotlib")
        
        logger.info(f"Initialized ParetoPlotter with {backend} backend")
    
    def plot_accuracy_vs_carbon(self,
                                agents: List,
                                frontier: List,
                                title: str = "Accuracy vs Carbon Footprint",
                                save_path: Optional[str] = None) -> Optional[go.Figure]:
        """
        Plot Accuracy vs Carbon - Sustainability perspective
        
        Policy Question:
        "What performance am I paying per unit environmental cost?"
        
        This plot reveals:
        - Which agents achieve high accuracy with low carbon
        - The carbon cost of marginal accuracy improvements
        - Green AI trade-offs
        
        Used by:
        - Sustainability reviewers
        - ESG compliance officers
        - Green AI researchers
        - Climate-conscious organizations
        
        Args:
            agents: All agents (ExtendedParetoPoint objects)
            frontier: Agents on Pareto frontier
            title: Plot title
            save_path: Optional path to save plot
        
        Returns:
            Plotly figure (if backend='plotly')
        
        Example:
            plotter = ParetoPlotter()
            fig = plotter.plot_accuracy_vs_carbon(agents, frontier)
            fig.show()  # Interactive plot
        """
        frontier_ids = {a.agent_id for a in frontier}
        
        if self.backend == 'plotly':
            fig = go.Figure()
            
            # Non-frontier agents (gray)
            non_frontier = [a for a in agents if a.agent_id not in frontier_ids]
            if non_frontier:
                fig.add_trace(go.Scatter(
                    x=[a.carbon_co2e_kg * 1000 for a in non_frontier],  # Convert to grams
                    y=[a.accuracy * 100 for a in non_frontier],  # Convert to percentage
                    mode='markers',
                    name='Dominated',
                    marker=dict(
                        size=10,
                        color='lightgray',
                        opacity=0.5
                    ),
                    text=[a.agent_id for a in non_frontier],
                    hovertemplate='%{text}<br>Carbon: %{x:.2f}g CO₂<br>Accuracy: %{y:.1f}%'
                ))
            
            # Frontier agents (green - fitting!)
            fig.add_trace(go.Scatter(
                x=[a.carbon_co2e_kg * 1000 for a in frontier],
                y=[a.accuracy * 100 for a in frontier],
                mode='markers+lines',
                name='Pareto Frontier',
                marker=dict(
                    size=15,
                    color='green',
                    symbol='star'
                ),
                line=dict(color='green', width=2, dash='dot'),
                text=[a.agent_id for a in frontier],
                hovertemplate='%{text}<br>Carbon: %{x:.2f}g CO₂<br>Accuracy: %{y:.1f}%'
            ))
            
            fig.update_layout(
                title=title,
                xaxis_title='Carbon Footprint (g CO₂e)',
                yaxis_title='Accuracy (%)',
                template='plotly_white',
                hovermode='closest',
                font=dict(size=12),
                showlegend=True,
                width=800,
                height=600
            )
            
            # Add diagonal reference lines (accuracy/carbon ratio)
            max_carbon = max([a.carbon_co2e_kg * 1000 for a in agents])
            for ratio in [50, 100, 200]:  # Accuracy % per gram
                fig.add_trace(go.Scatter(
                    x=[0, max_carbon],
                    y=[0, max_carbon * ratio / 1000],
                    mode='lines',
                    line=dict(color='gray', width=1, dash='dash'),
                    showlegend=False,
                    hoverinfo='skip'
                ))
            
            if save_path:
                fig.write_html(save_path)
                logger.info(f"Saved plot to {save_path}")
            
            return fig
        
        elif self.backend == 'matplotlib':
            fig, ax = plt.subplots(figsize=(10, 7))
            
            # Non-frontier
            non_frontier = [a for a in agents if a.agent_id not in frontier_ids]
            if non_frontier:
                ax.scatter(
                    [a.carbon_co2e_kg * 1000 for a in non_frontier],
                    [a.accuracy * 100 for a in non_frontier],
                    c='lightgray', s=100, alpha=0.5, label='Dominated'
                )
            
            # Frontier
            frontier_x = [a.carbon_co2e_kg * 1000 for a in frontier]
            frontier_y = [a.accuracy * 100 for a in frontier]
            ax.scatter(frontier_x, frontier_y, c='green', s=200, 
                      marker='*', label='Pareto Frontier', zorder=5)
            ax.plot(frontier_x, frontier_y, 'g--', alpha=0.5)
            
            # Labels
            for agent in frontier:
                ax.annotate(agent.agent_id, 
                           (agent.carbon_co2e_kg * 1000, agent.accuracy * 100),
                           xytext=(5, 5), textcoords='offset points')
            
            ax.set_xlabel('Carbon Footprint (g CO₂e)', fontsize=12)
            ax.set_ylabel('Accuracy (%)', fontsize=12)
            ax.set_title(title, fontsize=14, fontweight='bold')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                logger.info(f"Saved plot to {save_path}")
            
            return fig
    
    def plot_latency_vs_energy(self,
                               agents: List,
                               frontier: List,
                               title: str = "Latency vs Energy Consumption",
                               save_path: Optional[str] = None) -> Optional[go.Figure]:
        """
        Plot Latency vs Energy - Systems engineering perspective
        
        Policy Question:
        "Are fast agents inherently wasteful?"
        
        This plot reveals:
        - Whether low latency requires high energy (architectural insight)
        - Energy efficiency of fast agents
        - Real-time deployment viability
        
        Used by:
        - Systems engineers
        - Edge deployment teams
        - Real-time application developers
        - Performance architects
        
        Key Insight:
        If plot shows strong correlation: architecture couples speed and energy
        If plot shows weak correlation: algorithmic optimizations possible
        """
        frontier_ids = {a.agent_id for a in frontier}
        
        if self.backend == 'plotly':
            fig = go.Figure()
            
            # Non-frontier
            non_frontier = [a for a in agents if a.agent_id not in frontier_ids]
            if non_frontier:
                fig.add_trace(go.Scatter(
                    x=[a.latency_ms for a in non_frontier],
                    y=[a.energy_kwh * 1000 for a in non_frontier],  # Watt-hours
                    mode='markers',
                    name='Dominated',
                    marker=dict(size=10, color='lightgray', opacity=0.5),
                    text=[a.agent_id for a in non_frontier],
                    hovertemplate='%{text}<br>Latency: %{x:.0f}ms<br>Energy: %{y:.2f}Wh'
                ))
            
            # Frontier
            fig.add_trace(go.Scatter(
                x=[a.latency_ms for a in frontier],
                y=[a.energy_kwh * 1000 for a in frontier],
                mode='markers+lines',
                name='Pareto Frontier',
                marker=dict(size=15, color='blue', symbol='star'),
                line=dict(color='blue', width=2, dash='dot'),
                text=[a.agent_id for a in frontier],
                hovertemplate='%{text}<br>Latency: %{x:.0f}ms<br>Energy: %{y:.2f}Wh'
            ))
            
            fig.update_layout(
                title=title,
                xaxis_title='Latency (ms)',
                yaxis_title='Energy (Wh)',
                template='plotly_white',
                hovermode='closest',
                font=dict(size=12),
                showlegend=True,
                width=800,
                height=600
            )
            
            # Add SLA reference lines
            for sla_ms in [100, 500, 1000]:
                fig.add_vline(x=sla_ms, line_dash="dash", line_color="red", 
                             opacity=0.3, annotation_text=f"{sla_ms}ms SLA")
            
            if save_path:
                fig.write_html(save_path)
            
            return fig
        
        elif self.backend == 'matplotlib':
            fig, ax = plt.subplots(figsize=(10, 7))
            
            # Plot similar to plotly version
            non_frontier = [a for a in agents if a.agent_id not in frontier_ids]
            if non_frontier:
                ax.scatter([a.latency_ms for a in non_frontier],
                          [a.energy_kwh * 1000 for a in non_frontier],
                          c='lightgray', s=100, alpha=0.5, label='Dominated')
            
            frontier_x = [a.latency_ms for a in frontier]
            frontier_y = [a.energy_kwh * 1000 for a in frontier]
            ax.scatter(frontier_x, frontier_y, c='blue', s=200,
                      marker='*', label='Pareto Frontier', zorder=5)
            ax.plot(frontier_x, frontier_y, 'b--', alpha=0.5)
            
            ax.set_xlabel('Latency (ms)', fontsize=12)
            ax.set_ylabel('Energy (Wh)', fontsize=12)
            ax.set_title(title, fontsize=14, fontweight='bold')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
            
            return fig
    
    def plot_carbon_vs_energy(self,
                             agents: List,
                             frontier: List,
                             title: str = "Pure Green: Carbon vs Energy",
                             save_path: Optional[str] = None) -> Optional[go.Figure]:
        """
        Plot Carbon vs Energy - Pure environmental efficiency
        
        Policy Question:
        "Which agents are environmentally efficient independent of performance?"
        
        This is the MOST IMPORTANT plot for green AI.
        
        Why separate from accuracy:
        - Separates algorithmic efficiency from task difficulty
        - Shows pure environmental performance
        - Reveals hardware-algorithm mismatches
        - Independent of task success (measures resource efficiency)
        
        Used by:
        - Green AI researchers
        - Carbon budget planners
        - Environmental compliance officers
        - Sustainability engineers
        
        Key Insight:
        An agent can have:
        - High accuracy but poor green efficiency (upper right)
        - Low accuracy but excellent green efficiency (lower left)
        - This plot shows which agents are environmentally optimal
        """
        frontier_ids = {a.agent_id for a in frontier}
        
        if self.backend == 'plotly':
            fig = go.Figure()
            
            # Non-frontier
            non_frontier = [a for a in agents if a.agent_id not in frontier_ids]
            if non_frontier:
                # Color by accuracy for context
                fig.add_trace(go.Scatter(
                    x=[a.energy_kwh * 1000 for a in non_frontier],
                    y=[a.carbon_co2e_kg * 1000 for a in non_frontier],
                    mode='markers',
                    name='Dominated',
                    marker=dict(
                        size=10,
                        color=[a.accuracy for a in non_frontier],
                        colorscale='Viridis',
                        opacity=0.6,
                        showscale=True,
                        colorbar=dict(title="Accuracy")
                    ),
                    text=[f"{a.agent_id}<br>Acc: {a.accuracy:.1%}" for a in non_frontier],
                    hovertemplate='%{text}<br>Energy: %{x:.2f}Wh<br>Carbon: %{y:.2f}g'
                ))
            
            # Frontier (pure green frontier!)
            fig.add_trace(go.Scatter(
                x=[a.energy_kwh * 1000 for a in frontier],
                y=[a.carbon_co2e_kg * 1000 for a in frontier],
                mode='markers+lines',
                name='Green Frontier',
                marker=dict(
                    size=15,
                    color='darkgreen',
                    symbol='star',
                    line=dict(color='white', width=2)
                ),
                line=dict(color='darkgreen', width=3, dash='dot'),
                text=[f"{a.agent_id}<br>Acc: {a.accuracy:.1%}" for a in frontier],
                hovertemplate='%{text}<br>Energy: %{x:.2f}Wh<br>Carbon: %{y:.2f}g'
            ))
            
            # Add diagonal reference line (carbon intensity = carbon/energy)
            max_energy = max([a.energy_kwh * 1000 for a in agents])
            max_carbon = max([a.carbon_co2e_kg * 1000 for a in agents])
            
            # Typical grid carbon intensities (g CO₂/Wh)
            for intensity, label in [(0.2, 'US-CA Grid'), (0.6, 'CN Grid'), (0.05, 'FR Grid')]:
                fig.add_trace(go.Scatter(
                    x=[0, max_energy],
                    y=[0, max_energy * intensity],
                    mode='lines',
                    line=dict(color='gray', width=1, dash='dash'),
                    name=label,
                    showlegend=True,
                    hoverinfo='skip'
                ))
            
            fig.update_layout(
                title=title,
                xaxis_title='Energy (Wh)',
                yaxis_title='Carbon (g CO₂e)',
                template='plotly_white',
                hovermode='closest',
                font=dict(size=12),
                showlegend=True,
                width=800,
                height=600
            )
            
            # Add annotation explaining the plot
            fig.add_annotation(
                text="Lower-left = Greenest<br>Independent of accuracy",
                xref="paper", yref="paper",
                x=0.02, y=0.98,
                showarrow=False,
                bgcolor="lightgreen",
                opacity=0.8
            )
            
            if save_path:
                fig.write_html(save_path)
            
            return fig
        
        elif self.backend == 'matplotlib':
            fig, ax = plt.subplots(figsize=(10, 7))
            
            non_frontier = [a for a in agents if a.agent_id not in frontier_ids]
            if non_frontier:
                scatter = ax.scatter(
                    [a.energy_kwh * 1000 for a in non_frontier],
                    [a.carbon_co2e_kg * 1000 for a in non_frontier],
                    c=[a.accuracy for a in non_frontier],
                    s=100, alpha=0.6, cmap='viridis', label='Dominated'
                )
                plt.colorbar(scatter, label='Accuracy')
            
            frontier_x = [a.energy_kwh * 1000 for a in frontier]
            frontier_y = [a.carbon_co2e_kg * 1000 for a in frontier]
            ax.scatter(frontier_x, frontier_y, c='darkgreen', s=200,
                      marker='*', label='Green Frontier', zorder=5, 
                      edgecolors='white', linewidths=2)
            ax.plot(frontier_x, frontier_y, 'g--', linewidth=2, alpha=0.7)
            
            ax.set_xlabel('Energy (Wh)', fontsize=12)
            ax.set_ylabel('Carbon (g CO₂e)', fontsize=12)
            ax.set_title(title, fontsize=14, fontweight='bold')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # Add text box
            textstr = 'Lower-left = Greenest\nIndependent of accuracy'
            props = dict(boxstyle='round', facecolor='lightgreen', alpha=0.8)
            ax.text(0.02, 0.98, textstr, transform=ax.transAxes, fontsize=10,
                   verticalalignment='top', bbox=props)
            
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
            
            return fig
    
    def plot_all_projections(self,
                            agents: List,
                            frontier: List,
                            save_dir: Optional[str] = None) -> Dict[str, go.Figure]:
        """
        Generate all three specialized plots
        
        Returns a dashboard with:
        1. Accuracy vs Carbon (sustainability view)
        2. Latency vs Energy (systems view)
        3. Carbon vs Energy (pure green view)
        
        Args:
            agents: All agents
            frontier: Pareto frontier
            save_dir: Directory to save plots
        
        Returns:
            Dict mapping plot_name -> figure
        """
        plots = {}
        
        # Generate all three plots
        plots['accuracy_vs_carbon'] = self.plot_accuracy_vs_carbon(
            agents, frontier,
            save_path=f"{save_dir}/accuracy_vs_carbon.html" if save_dir else None
        )
        
        plots['latency_vs_energy'] = self.plot_latency_vs_energy(
            agents, frontier,
            save_path=f"{save_dir}/latency_vs_energy.html" if save_dir else None
        )
        
        plots['carbon_vs_energy'] = self.plot_carbon_vs_energy(
            agents, frontier,
            save_path=f"{save_dir}/carbon_vs_energy.html" if save_dir else None
        )
        
        logger.info(f"Generated {len(plots)} projection plots")
        return plots
