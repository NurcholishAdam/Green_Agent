# File: helium_visualization.py

"""
Interactive Helium Market Dashboard
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class HeliumMarketDashboard:
    """Interactive dashboard for helium market visualization"""
    
    def __init__(self, data_path: str = "./data/helium_timeseries.csv"):
        self.df = pd.read_csv(data_path, parse_dates=['date'])
        self.forecasts = None
        
        try:
            self.forecasts = pd.read_csv("./data/helium_forecasts.csv", parse_dates=['date'])
        except:
            pass
        
        self._calculate_metrics()
    
    def _calculate_metrics(self):
        """Calculate additional metrics for visualization"""
        self.df['deficit'] = self.df['global_demand_tonnes'] - self.df['global_production_tonnes']
        self.df['price_change'] = self.df['price_index'].pct_change() * 100
        
        # Market regime classification
        conditions = [
            (self.df['scarcity_index'] < 0.3),
            (self.df['scarcity_index'] >= 0.3) & (self.df['scarcity_index'] < 0.6),
            (self.df['scarcity_index'] >= 0.6) & (self.df['scarcity_index'] < 0.8),
            (self.df['scarcity_index'] >= 0.8)
        ]
        regimes = ['Low Scarcity', 'Moderate Scarcity', 'High Scarcity', 'Critical Scarcity']
        self.df['market_regime'] = np.select(conditions, regimes)
    
    def create_supply_demand_chart(self) -> go.Figure:
        """Create supply-demand trend chart"""
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
            title='Helium Supply-Demand Dynamics (2020-2026)',
            xaxis_title='Date',
            yaxis_title='Tonnes per Year',
            yaxis2=dict(title='Deficit (Tonnes)', overlaying='y', side='right'),
            hovermode='x unified',
            template='plotly_white',
            height=500
        )
        
        return fig
    
    def create_scarcity_price_heatmap(self) -> go.Figure:
        """Create scarcity-price correlation heatmap"""
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=self.df['scarcity_index'], y=self.df['price_index'],
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
        z = np.polyfit(self.df['scarcity_index'], self.df['price_index'], 1)
        p = np.poly1d(z)
        x_trend = np.linspace(self.df['scarcity_index'].min(), self.df['scarcity_index'].max(), 100)
        
        fig.add_trace(go.Scatter(
            x=x_trend, y=p(x_trend),
            mode='lines', name=f'Trend: R²={np.corrcoef(self.df["scarcity_index"], self.df["price_index"])[0,1]**2:.3f}',
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
        """Create risk radar chart for latest period"""
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
        
        # Add historical baseline (2020)
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
            name='Baseline (2020)'
        ))
        
        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            title='Helium Risk Assessment Dashboard',
            template='plotly_white',
            height=500
        )
        
        return fig
    
    def create_forecast_chart(self) -> go.Figure:
        """Create forecast chart for future projections"""
        fig = go.Figure()
        
        # Historical data
        fig.add_trace(go.Scatter(
            x=self.df['date'], y=self.df['scarcity_index'],
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
            
            # Add confidence interval
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
            title='Helium Scarcity Forecast (2026-2030)',
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
        """Create circular economy progress chart"""
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
    
    def create_kpi_dashboard(self) -> Dict:
        """Generate KPI cards for dashboard"""
        latest = self.df.iloc[-1]
        prev = self.df.iloc[-2]
        
        kpis = {
            'Current Scarcity': {
                'value': f"{latest['scarcity_index']:.2f}",
                'change': f"{(latest['scarcity_index'] - prev['scarcity_index'])*100:+.1f}%",
                'trend': 'up' if latest['scarcity_index'] > prev['scarcity_index'] else 'down',
                'color': 'red' if latest['scarcity_index'] > 0.7 else 'orange' if latest['scarcity_index'] > 0.4 else 'green'
            },
            'Price Index': {
                'value': f"{latest['price_index']:.0f}",
                'change': f"{(latest['price_index'] - prev['price_index']):+.0f} pts",
                'trend': 'up' if latest['price_index'] > prev['price_index'] else 'down',
                'color': 'red' if latest['price_index'] > 150 else 'green'
            },
            'Supply-Demand Gap': {
                'value': f"{latest['deficit']:+,.0f} t",
                'change': f"{(latest['deficit'] - prev['deficit']):+,.0f}",
                'trend': 'up' if latest['deficit'] > prev['deficit'] else 'down',
                'color': 'red' if latest['deficit'] > 0 else 'green'
            },
            'Recycling Rate': {
                'value': f"{latest['recycling_rate_0_1']:.1%}",
                'change': f"{(latest['recycling_rate_0_1'] - prev['recycling_rate_0_1'])*100:+.1f}%",
                'trend': 'up' if latest['recycling_rate_0_1'] > prev['recycling_rate_0_1'] else 'down',
                'color': 'green' if latest['recycling_rate_0_1'] > 0.2 else 'orange'
            }
        }
        
        return kpis
    
    def generate_html_dashboard(self, output_file: str = "helium_dashboard.html"):
        """Generate complete HTML dashboard"""
        import plotly.io as pio
        
        kpis = self.create_kpi_dashboard()
        
        # Create KPI HTML
        kpi_html = '<div style="display: flex; gap: 20px; margin-bottom: 30px;">'
        for name, kpi in kpis.items():
            color = kpi['color']
            kpi_html += f'''
            <div style="flex: 1; background: {color if color == 'green' else '#FFF'}; padding: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); border-left: 4px solid {color};">
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
                        <li><strong>Critical Threshold Alert:</strong> Scarcity index currently at {self.df.iloc[-1]['scarcity_index']:.2f} - {'⚠️ Critical' if self.df.iloc[-1]['scarcity_index'] > 0.7 else 'Stable'}</li>
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
        
        print(f"✅ Dashboard generated: {output_file}")
        return output_file

# Generate dashboard
if __name__ == "__main__":
    dashboard = HeliumMarketDashboard()
    dashboard.generate_html_dashboard()
    print("\n📊 Dashboard created successfully! Open 'helium_dashboard.html' in your browser.")
