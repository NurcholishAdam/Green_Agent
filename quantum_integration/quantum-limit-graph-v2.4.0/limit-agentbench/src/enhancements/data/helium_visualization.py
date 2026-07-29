# File: helium_visualization.py
# Version: 2.0.0
"""
Interactive Helium Market Dashboard with enhanced configurability and robustness.

ENHANCEMENTS OVER v1.0:
- Configurable data paths via environment variables and CLI arguments.
- Robust file handling with fallback to generated synthetic data.
- Ability to generate specific charts or full dashboard.
- Improved KPI color logic and additional KPIs.
- Export individual charts as HTML/PNG via command line.
- Logging and error handling.
- Option to serve as a web server (Dash).
"""

import os
import sys
import argparse
import logging
import warnings
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Union

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import plotly.io as pio

# For optional web server
try:
    import dash
    from dash import dcc, html, Input, Output
    DASH_AVAILABLE = True
except ImportError:
    DASH_AVAILABLE = False

warnings.filterwarnings('ignore')

# ============================================================================
# Configuration
# ============================================================================
class Config:
    """Central configuration with environment variable support."""
    DATA_PATH = os.getenv('HELIUM_DASHBOARD_DATA', './data/helium_timeseries.csv')
    FORECAST_PATH = os.getenv('HELIUM_DASHBOARD_FORECAST', './data/helium_forecasts.csv')
    OUTPUT_PATH = os.getenv('HELIUM_DASHBOARD_OUTPUT', './helium_dashboard.html')
    LOG_LEVEL = os.getenv('HELIUM_DASHBOARD_LOG', 'INFO')

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Synthetic Data Generator (fallback)
# ============================================================================
def generate_synthetic_data(n_periods: int = 60, start_date: str = "2020-01-01") -> pd.DataFrame:
    """Generate synthetic helium data for demonstration when file not found."""
    logger.info("Generating synthetic data for demo purposes.")
    np.random.seed(42)
    dates = pd.date_range(start=start_date, periods=n_periods, freq='M')
    t = np.arange(n_periods)

    production = np.clip(28000 - t * 40 + np.random.normal(0, 300, n_periods), 20000, 35000)
    demand = np.clip(27000 + t * 80 + np.random.normal(0, 400, n_periods), 25000, 45000)
    price = 100 * np.exp(np.cumsum(np.random.normal(0.005, 0.1, n_periods)))
    seasonal = 1 + 0.1 * np.sin(2 * np.pi * t / 12)
    price = price * seasonal
    price = np.clip(price, 50, 500)
    demand_supply_ratio = demand / production
    shortage = np.clip((demand_supply_ratio - 0.95) * 4, 0.05, 1.0)
    supply_risk = np.clip(0.2 + t * 0.002 + 0.1 * np.sin(2 * np.pi * t / 24) + np.random.normal(0, 0.05, n_periods), 0.1, 0.9)
    recycling = np.clip(0.10 + t * 0.003 + np.random.normal(0, 0.01, n_periods), 0.05, 0.40)
    substitution = np.clip(0.08 + t * 0.004 + np.random.normal(0, 0.01, n_periods), 0.05, 0.50)
    cooling = np.clip(0.85 + t * 0.005 + np.random.normal(0, 0.02, n_periods), 0.7, 1.3)
    geo_risk = np.clip(0.3 + 0.2 * np.sin(2 * np.pi * t / 36) + np.random.normal(0, 0.05, n_periods), 0.1, 0.8)
    logistics = np.clip(0.2 + t * 0.001 + np.random.normal(0, 0.05, n_periods), 0.1, 0.7)
    new_capacity = np.maximum(500, 2000 + t * 100 + np.random.normal(0, 200, n_periods))

    scarcity_impact = np.clip(shortage * 0.6 + supply_risk * 0.4, 0, 1)
    price_volatility = pd.Series(price).rolling(6).std().fillna(5).values
    price_volatility = np.clip(price_volatility, 1, 30)
    market_regime = []
    for sc in scarcity_impact:
        if sc > 0.7: regime = "crisis"
        elif sc > 0.5: regime = "tightening"
        elif sc > 0.3: regime = "normal"
        else: regime = "stable"
        market_regime.append(regime)
    carbon_intensity = np.clip(300 + 200 * scarcity_impact + np.random.normal(0, 50, n_periods), 50, 800)
    renewable_pct = np.clip(30 + 40 * (1 - scarcity_impact) + np.random.normal(0, 10, n_periods), 5, 95)
    circularity_potential = (recycling + substitution) / 2
    thermal_impact = cooling * scarcity_impact
    future_supply_potential = np.clip((new_capacity / production) * 100, 0, 50)
    capacity_utilization = production / (production + new_capacity)
    esg_score = np.clip((recycling * 40 + (1 - supply_risk) * 30 + (1 - geo_risk) * 30) * 100, 0, 100)
    regulatory_risk = np.clip(geo_risk * 0.5 + logistics * 0.5, 0, 1)

    df = pd.DataFrame({
        'date': dates,
        'global_production_tonnes': np.round(production, 0),
        'global_demand_tonnes': np.round(demand, 0),
        'price_index': np.round(price, 1),
        'shortage_severity_0_1': np.round(shortage, 3),
        'supply_risk_score_0_1': np.round(supply_risk, 3),
        'recycling_rate_0_1': np.round(recycling, 3),
        'substitution_feasibility_0_1': np.round(substitution, 3),
        'cooling_load_sensitivity': np.round(cooling, 3),
        'geopolitical_risk_index': np.round(geo_risk, 3),
        'logistics_disruption_index': np.round(logistics, 3),
        'new_production_capacity_tonnes': np.round(new_capacity, 0),
        'helium_scarcity_impact': np.round(scarcity_impact, 3),
        'price_volatility': np.round(price_volatility, 2),
        'market_regime': market_regime,
        'carbon_intensity_associated': np.round(carbon_intensity, 0),
        'renewable_energy_pct': np.round(renewable_pct, 1),
        'demand_supply_ratio': np.round(demand_supply_ratio, 3),
        'circularity_potential': np.round(circularity_potential, 3),
        'thermal_impact_factor': np.round(thermal_impact, 3),
        'future_supply_potential_pct': np.round(future_supply_potential, 1),
        'capacity_utilization_rate': np.round(capacity_utilization, 3),
        'esg_score': np.round(esg_score, 1),
        'regulatory_risk_score': np.round(regulatory_risk, 3)
    })
    return df


# ============================================================================
# HeliumMarketDashboard (Enhanced)
# ============================================================================
class HeliumMarketDashboard:
    """Interactive dashboard for helium market visualization with enhanced configurability."""

    def __init__(
        self,
        data_path: Optional[str] = None,
        forecast_path: Optional[str] = None,
        generate_synthetic_fallback: bool = True,
    ):
        """
        Initialize the dashboard.

        Args:
            data_path: Path to CSV data file. If None, uses Config.DATA_PATH.
            forecast_path: Path to CSV forecast file. If None, uses Config.FORECAST_PATH.
            generate_synthetic_fallback: If True, generate synthetic data when file not found.
        """
        self.data_path = data_path or Config.DATA_PATH
        self.forecast_path = forecast_path or Config.FORECAST_PATH
        self.generate_synthetic_fallback = generate_synthetic_fallback

        self.df = None
        self.forecasts = None

        self._load_data()
        self._load_forecast()
        self._calculate_metrics()

        logger.info("HeliumMarketDashboard initialized.")

    def _load_data(self):
        """Load main data from CSV or fallback to synthetic."""
        if os.path.exists(self.data_path):
            try:
                self.df = pd.read_csv(self.data_path, parse_dates=['date'])
                logger.info(f"Loaded data from {self.data_path}")
                return
            except Exception as e:
                logger.warning(f"Failed to load data: {e}")

        if self.generate_synthetic_fallback:
            self.df = generate_synthetic_data()
            logger.info("Using synthetic data.")
        else:
            raise FileNotFoundError(f"Data file not found: {self.data_path}")

    def _load_forecast(self):
        """Load forecast data if available."""
        if os.path.exists(self.forecast_path):
            try:
                self.forecasts = pd.read_csv(self.forecast_path, parse_dates=['date'])
                logger.info(f"Loaded forecasts from {self.forecast_path}")
            except Exception as e:
                logger.warning(f"Failed to load forecasts: {e}")
                self.forecasts = None
        else:
            logger.info("No forecast file found. Forecast chart will be omitted.")

    def _calculate_metrics(self):
        """Calculate additional metrics for visualization."""
        if self.df is None:
            return
        self.df['deficit'] = self.df['global_demand_tonnes'] - self.df['global_production_tonnes']
        self.df['price_change'] = self.df['price_index'].pct_change() * 100

        # Market regime classification
        conditions = [
            (self.df['helium_scarcity_impact'] < 0.3),
            (self.df['helium_scarcity_impact'] >= 0.3) & (self.df['helium_scarcity_impact'] < 0.6),
            (self.df['helium_scarcity_impact'] >= 0.6) & (self.df['helium_scarcity_impact'] < 0.8),
            (self.df['helium_scarcity_impact'] >= 0.8)
        ]
        regimes = ['Low Scarcity', 'Moderate Scarcity', 'High Scarcity', 'Critical Scarcity']
        self.df['market_regime'] = np.select(conditions, regimes)

    # ---------- Chart generation methods ----------
    def create_supply_demand_chart(self) -> go.Figure:
        """Create supply-demand trend chart."""
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=self.df['date'], y=self.df['global_production_tonnes'],
            mode='lines+markers', name='Production',
            line=dict(color='green', width=3),
            marker=dict(size=8)
        ))

        fig.add_trace(go.Scatter(
            x=self.df['date'], y=self.df['global_demand_tonnes'],
            mode='lines+markers', name='Demand',
            line=dict(color='red', width=3),
            marker=dict(size=8)
        ))

        # Add deficit area
        fig.add_trace(go.Scatter(
            x=self.df['date'], y=self.df['deficit'],
            fill='tozeroy', name='Deficit (Demand - Production)',
            line=dict(color='orange', width=2, dash='dot'),
            yaxis='y2'
        ))

        fig.update_layout(
            title='Helium Supply-Demand Dynamics',
            xaxis_title='Date',
            yaxis_title='Tonnes per Year',
            yaxis2=dict(title='Deficit (Tonnes)', overlaying='y', side='right'),
            hovermode='x unified',
            template='plotly_white',
            height=500
        )

        return fig

    def create_scarcity_price_heatmap(self) -> go.Figure:
        """Create scarcity-price correlation heatmap."""
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=self.df['helium_scarcity_impact'], y=self.df['price_index'],
            mode='markers+text',
            marker=dict(
                size=self.df['global_production_tonnes']/1000,
                color=self.df['date'].dt.year,
                colorscale='Viridis',
                showscale=True,
                colorbar=dict(title="Year")
            ),
            text=self.df['date'].dt.year,
            textposition="top center",
            name='Market Points'
        ))

        # Add trend line
        z = np.polyfit(self.df['helium_scarcity_impact'], self.df['price_index'], 1)
        p = np.poly1d(z)
        x_trend = np.linspace(self.df['helium_scarcity_impact'].min(), self.df['helium_scarcity_impact'].max(), 100)

        fig.add_trace(go.Scatter(
            x=x_trend, y=p(x_trend),
            mode='lines', name=f'Trend: R²={np.corrcoef(self.df["helium_scarcity_impact"], self.df["price_index"])[0,1]**2:.3f}',
            line=dict(color='red', dash='dash')
        ))

        fig.update_layout(
            title='Scarcity vs Price Correlation',
            xaxis_title='Scarcity Index (0-1)',
            yaxis_title='Price Index (Base=100)',
            template='plotly_white',
            height=500
        )

        return fig

    def create_risk_radar(self) -> go.Figure:
        """Create risk radar chart for latest period."""
        latest = self.df.iloc[-1]

        categories = ['Supply Risk', 'Geopolitical Risk', 'Logistics Risk',
                     'Shortage Severity', 'Price Volatility', 'Cooling Sensitivity']

        values = [
            latest['supply_risk_score_0_1'],
            latest['geopolitical_risk_index'],
            latest['logistics_disruption_index'],
            latest['shortage_severity_0_1'],
            abs(latest['price_change']) / 100 if not pd.isna(latest['price_change']) else 0.1,
            latest['cooling_load_sensitivity'] / 1.5
        ]

        fig = go.Figure(data=go.Scatterpolar(
            r=values,
            theta=categories,
            fill='toself',
            marker=dict(color='red', size=8),
            name=f'Current Risks ({latest["date"].year})'
        ))

        # Add historical baseline (first year)
        baseline = self.df.iloc[0]
        baseline_values = [
            baseline['supply_risk_score_0_1'],
            baseline['geopolitical_risk_index'],
            baseline['logistics_disruption_index'],
            baseline['shortage_severity_0_1'],
            0.05,
            baseline['cooling_load_sensitivity'] / 1.5
        ]

        fig.add_trace(go.Scatterpolar(
            r=baseline_values,
            theta=categories,
            fill='toself',
            marker=dict(color='blue', size=8),
            name=f'Baseline ({baseline["date"].year})'
        ))

        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            title='Helium Risk Assessment Dashboard',
            template='plotly_white',
            height=500
        )

        return fig

    def create_forecast_chart(self) -> go.Figure:
        """Create forecast chart for future projections."""
        fig = go.Figure()

        # Historical data
        fig.add_trace(go.Scatter(
            x=self.df['date'], y=self.df['helium_scarcity_impact'],
            mode='lines+markers', name='Historical Scarcity',
            line=dict(color='blue', width=3),
            marker=dict(size=8)
        ))

        # Forecast data if available
        if self.forecasts is not None:
            fig.add_trace(go.Scatter(
                x=self.forecasts['date'], y=self.forecasts['scarcity_index'],
                mode='lines', name='Forecast Scarcity',
                line=dict(color='red', width=3, dash='dash')
            ))

            # Add confidence interval (simulated)
            fig.add_trace(go.Scatter(
                x=self.forecasts['date'],
                y=self.forecasts['scarcity_index'] * 1.1,
                mode='lines', name='Upper Bound',
                line=dict(color='rgba(255,0,0,0.2)', width=0),
                showlegend=False
            ))
            fig.add_trace(go.Scatter(
                x=self.forecasts['date'],
                y=self.forecasts['scarcity_index'] * 0.9,
                mode='lines', name='Lower Bound',
                fill='tonexty',
                line=dict(color='rgba(255,0,0,0.2)', width=0),
                showlegend=False
            ))

        fig.update_layout(
            title='Helium Scarcity Forecast',
            xaxis_title='Date',
            yaxis_title='Scarcity Index (0-1)',
            template='plotly_white',
            height=500,
            annotations=[
                dict(
                    x=0.5, y=0.9, xref='paper', yref='paper',
                    text='⚠️ Critical threshold: >0.8 indicates severe shortage',
                    showarrow=False,
                    font=dict(color='red', size=12)
                )
            ]
        )

        return fig

    def create_circularity_progress(self) -> go.Figure:
        """Create circular economy progress chart."""
        fig = make_subplots(rows=1, cols=2,
                           subplot_titles=('Recycling Rate Progress', 'Circularity Potential'))

        # Recycling rate
        fig.add_trace(go.Scatter(
            x=self.df['date'], y=self.df['recycling_rate_0_1'],
            mode='lines+markers', name='Recycling Rate',
            line=dict(color='green', width=3),
            fill='tozeroy'
        ), row=1, col=1)

        # Target line
        fig.add_hline(y=0.50, line_dash="dash", line_color="red",
                     annotation_text="2030 Target (50%)", row=1, col=1)

        # Circularity potential
        fig.add_trace(go.Scatter(
            x=self.df['date'], y=self.df['circularity_potential'],
            mode='lines+markers', name='Circularity Potential',
            line=dict(color='blue', width=3),
            fill='tozeroy'
        ), row=1, col=2)

        fig.update_layout(
            title='Helium Circular Economy Progress',
            template='plotly_white',
            height=500,
            showlegend=True
        )

        return fig

    # ---------- KPI and dashboard generation ----------
    def create_kpi_dashboard(self) -> Dict:
        """Generate KPI cards for dashboard."""
        if self.df is None or len(self.df) < 2:
            return {}

        latest = self.df.iloc[-1]
        prev = self.df.iloc[-2]

        # Improved color logic using thresholds
        def get_color(value, thresholds, colors):
            for thr, col in zip(thresholds, colors):
                if value <= thr:
                    return col
            return colors[-1]

        scarcity_color = get_color(latest['helium_scarcity_impact'], [0.3, 0.6, 0.8], ['green', 'orange', 'red', 'red'])
        price_color = get_color(latest['price_index'], [100, 150, 200], ['green', 'orange', 'red'])
        deficit_color = 'red' if latest['deficit'] > 0 else 'green'
        recycling_color = get_color(latest['recycling_rate_0_1'], [0.2, 0.35, 0.5], ['red', 'orange', 'green'])

        kpis = {
            'Scarcity Index': {
                'value': f"{latest['helium_scarcity_impact']:.2f}",
                'change': f"{(latest['helium_scarcity_impact'] - prev['helium_scarcity_impact'])*100:+.1f}%",
                'trend': 'up' if latest['helium_scarcity_impact'] > prev['helium_scarcity_impact'] else 'down',
                'color': scarcity_color
            },
            'Price Index': {
                'value': f"{latest['price_index']:.0f}",
                'change': f"{(latest['price_index'] - prev['price_index']):+.0f} pts",
                'trend': 'up' if latest['price_index'] > prev['price_index'] else 'down',
                'color': price_color
            },
            'Supply-Demand Gap': {
                'value': f"{latest['deficit']:+,.0f} t",
                'change': f"{(latest['deficit'] - prev['deficit']):+,.0f}",
                'trend': 'up' if latest['deficit'] > prev['deficit'] else 'down',
                'color': deficit_color
            },
            'Recycling Rate': {
                'value': f"{latest['recycling_rate_0_1']:.1%}",
                'change': f"{(latest['recycling_rate_0_1'] - prev['recycling_rate_0_1'])*100:+.1f}%",
                'trend': 'up' if latest['recycling_rate_0_1'] > prev['recycling_rate_0_1'] else 'down',
                'color': recycling_color
            }
        }

        return kpis

    def generate_html_dashboard(self, output_file: Optional[str] = None) -> str:
        """
        Generate complete HTML dashboard.

        Args:
            output_file: Output HTML file path. If None, uses Config.OUTPUT_PATH.

        Returns:
            Path to the generated file.
        """
        if output_file is None:
            output_file = Config.OUTPUT_PATH

        kpis = self.create_kpi_dashboard()

        # Create KPI HTML
        kpi_html = '<div style="display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 30px;">'
        for name, kpi in kpis.items():
            color = kpi['color']
            kpi_html += f'''
            <div style="flex: 1; min-width: 200px; background: {color if color == 'green' else '#FFF'}; padding: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); border-left: 4px solid {color};">
                <h3 style="margin: 0; font-size: 14px; color: #666;">{name}</h3>
                <p style="margin: 10px 0; font-size: 28px; font-weight: bold;">{kpi['value']}</p>
                <p style="margin: 0; font-size: 12px; color: {color if kpi['trend'] == 'up' else 'green'}">
                    {kpi['change']} vs previous period
                </p>
            </div>
            '''
        kpi_html += '</div>'

        # Generate charts
        supply_demand = self.create_supply_demand_chart()
        scarcity_price = self.create_scarcity_price_heatmap()
        risk_radar = self.create_risk_radar()
        forecast = self.create_forecast_chart()
        circularity = self.create_circularity_progress()

        # Combine into single HTML
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Helium Market Intelligence Dashboard</title>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
                .container {{ max-width: 1400px; margin: 0 auto; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; }}
                .chart-container {{ background: white; padding: 20px; border-radius: 10px; margin-bottom: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                h1 {{ margin: 0; }}
                .subtitle {{ margin: 10px 0 0; opacity: 0.9; }}
                @media (max-width: 768px) {{
                    .kpi-container {{ flex-direction: column; }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📈 Helium Market Intelligence Dashboard</h1>
                    <p class="subtitle">Real-time market monitoring & predictive analytics | Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>

                {kpi_html}

                <div class="chart-container">
                    {pio.to_html(supply_demand, full_html=False, config={'displayModeBar': False})}
                </div>

                <div class="chart-container">
                    {pio.to_html(scarcity_price, full_html=False, config={'displayModeBar': False})}
                </div>

                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 30px; margin-bottom: 30px;">
                    <div class="chart-container" style="margin-bottom: 0;">
                        {pio.to_html(risk_radar, full_html=False, config={'displayModeBar': False})}
                    </div>
                    <div class="chart-container" style="margin-bottom: 0;">
                        {pio.to_html(circularity, full_html=False, config={'displayModeBar': False})}
                    </div>
                </div>

                <div class="chart-container">
                    {pio.to_html(forecast, full_html=False, config={'displayModeBar': False})}
                </div>

                <div class="chart-container">
                    <h3>📊 Market Insights</h3>
                    <ul>
                        <li><strong>Critical Threshold Alert:</strong> Scarcity index currently at {self.df.iloc[-1]['helium_scarcity_impact']:.2f} - {'⚠️ Critical' if self.df.iloc[-1]['helium_scarcity_impact'] > 0.7 else 'Stable'}</li>
                        <li><strong>Supply-Demand Gap:</strong> {self.df.iloc[-1]['deficit']:+,.0f} tonnes - {'Deficit' if self.df.iloc[-1]['deficit'] > 0 else 'Surplus'}</li>
                        <li><strong>Recycling Progress:</strong> {self.df.iloc[-1]['recycling_rate_0_1']:.1%} of target (2030: 50%)</li>
                        <li><strong>Price Forecast:</strong> Expected to {'increase' if self.forecasts is not None and self.forecasts['price_index'].iloc[-1] > self.df['price_index'].iloc[-1] else 'stabilize'} in coming years</li>
                    </ul>
                </div>
            </div>
        </body>
        </html>
        """

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        logger.info(f"Dashboard generated: {output_file}")
        return output_file

    # ---------- Chart export methods ----------
    def export_chart(self, chart_name: str, output_file: str, format: str = 'html'):
        """
        Export a single chart to HTML or PNG.

        Args:
            chart_name: Name of the chart (supply_demand, scarcity_price, risk_radar, forecast, circularity).
            output_file: Output file path.
            format: 'html' or 'png' (requires kaleido).
        """
        chart_map = {
            'supply_demand': self.create_supply_demand_chart,
            'scarcity_price': self.create_scarcity_price_heatmap,
            'risk_radar': self.create_risk_radar,
            'forecast': self.create_forecast_chart,
            'circularity': self.create_circularity_progress,
        }

        if chart_name not in chart_map:
            raise ValueError(f"Unknown chart name: {chart_name}. Available: {list(chart_map.keys())}")

        fig = chart_map[chart_name]()
        if format == 'html':
            pio.write_html(fig, output_file)
        elif format == 'png':
            try:
                pio.write_image(fig, output_file)
            except ImportError:
                logger.error("kaleido not installed. Cannot export as PNG. Install with: pip install kaleido")
                raise
        else:
            raise ValueError("Format must be 'html' or 'png'")

        logger.info(f"Exported {chart_name} to {output_file}")

    # ---------- Interactive web server (Dash) ----------
    def serve_dash(self, host: str = '127.0.0.1', port: int = 8050):
        """
        Serve the dashboard as a Dash web app.

        Requires dash and dash-bootstrap-components (optional).
        """
        if not DASH_AVAILABLE:
            raise ImportError("Dash not installed. Install with: pip install dash")

        from dash import dcc, html, Input, Output

        app = dash.Dash(__name__, title="Helium Dashboard")

        app.layout = html.Div([
            html.H1("Helium Market Dashboard", style={'textAlign': 'center'}),
            dcc.Graph(id='supply-demand', figure=self.create_supply_demand_chart()),
            dcc.Graph(id='scarcity-price', figure=self.create_scarcity_price_heatmap()),
            html.Div([
                dcc.Graph(id='risk-radar', figure=self.create_risk_radar()),
                dcc.Graph(id='circularity', figure=self.create_circularity_progress()),
            ], style={'display': 'grid', 'gridTemplateColumns': '1fr 1fr', 'gap': '20px'}),
            dcc.Graph(id='forecast', figure=self.create_forecast_chart()),
            html.Div(id='kpi-display'),
            dcc.Interval(id='interval-component', interval=300000, n_intervals=0)  # refresh every 5 min
        ])

        @app.callback(
            Output('kpi-display', 'children'),
            Input('interval-component', 'n_intervals')
        )
        def update_kpis(_):
            kpis = self.create_kpi_dashboard()
            kpi_divs = []
            for name, kpi in kpis.items():
                color = kpi['color']
                kpi_divs.append(html.Div([
                    html.H3(name),
                    html.P(kpi['value']),
                    html.P(f"{kpi['change']} vs previous", style={'color': color})
                ], style={'flex': 1, 'border': f'1px solid {color}', 'padding': '10px', 'borderRadius': '5px'}))
            return html.Div(kpi_divs, style={'display': 'flex', 'gap': '10px', 'flexWrap': 'wrap'})

        logger.info(f"Starting Dash server at http://{host}:{port}")
        app.run_server(host=host, port=port, debug=False)


# ============================================================================
# CLI Interface
# ============================================================================
def parse_args():
    parser = argparse.ArgumentParser(description="Helium Market Dashboard")
    parser.add_argument('--data', default=Config.DATA_PATH, help='Path to CSV data file')
    parser.add_argument('--forecast', default=Config.FORECAST_PATH, help='Path to CSV forecast file')
    parser.add_argument('--output', default=Config.OUTPUT_PATH, help='Output HTML file path')
    parser.add_argument('--no-fallback', action='store_true', help='Disable synthetic fallback')
    parser.add_argument('--serve', action='store_true', help='Serve as web app via Dash')
    parser.add_argument('--port', type=int, default=8050, help='Port for Dash server')
    parser.add_argument('--export-chart', choices=['supply_demand', 'scarcity_price', 'risk_radar', 'forecast', 'circularity'],
                        help='Export a single chart')
    parser.add_argument('--export-format', choices=['html', 'png'], default='html', help='Export format')
    parser.add_argument('--export-output', default='chart.html', help='Output file for exported chart')
    return parser.parse_args()


def main():
    args = parse_args()

    dashboard = HeliumMarketDashboard(
        data_path=args.data,
        forecast_path=args.forecast,
        generate_synthetic_fallback=not args.no_fallback,
    )

    if args.export_chart:
        dashboard.export_chart(args.export_chart, args.export_output, args.export_format)
        return

    if args.serve:
        dashboard.serve_dash(port=args.port)
    else:
        dashboard.generate_html_dashboard(args.output)
        print(f"Dashboard generated: {args.output}. Open in browser.")


if __name__ == "__main__":
    main()
