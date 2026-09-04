import requests
import plotly.express as px
import pandas as pd

API_URL = "http://localhost:8000/pareto"

def plot_pareto(include_enhancements: bool = False):
    """
    Fetch Pareto frontier data from the Green Agent dashboard API
    and create an interactive Plotly plot.

    If include_enhancements is True, the script will also attempt to
    fetch MODP scores, RLHF feedback, graph metrics, and distillation stats
    from related endpoints (if available) and overlay them as annotations.
    """
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        pareto_data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch Pareto data from {API_URL}: {e}")
        return

    frontier_points = pareto_data.get('frontier_points', [])
    if not frontier_points:
        print("⚠️ No Pareto frontier points available.")
        return

    df = pd.DataFrame(frontier_points)
    required_cols = {'accuracy', 'energy'}
    if not required_cols.issubset(df.columns):
        print(f"❌ Missing required columns: {required_cols - set(df.columns)}")
        return

    # Create base scatter plot
    fig = px.scatter(
        df,
        x='energy',
        y='accuracy',
        color='agent_id' if 'agent_id' in df.columns else None,
        title='Pareto Frontier - Accuracy vs Energy',
        labels={'energy': 'Energy (kWh)', 'accuracy': 'Accuracy'},
        hover_data=df.columns
    )

    # Optional enhancement overlays
    if include_enhancements:
        # Attempt to fetch enhancement data from additional endpoints
        enhancement_endpoints = {
            'modp_scores': 'http://localhost:8000/analytics/modp',
            'rlhf_feedback': 'http://localhost:8000/analytics/rlhf',
            'graph_metrics': 'http://localhost:8000/analytics/graph',
            'distillation_stats': 'http://localhost:8000/analytics/distillation',
        }
        for name, url in enhancement_endpoints.items():
            try:
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    # We don't know exact structure; just print info for now
                    print(f"📊 {name}: {data}")
                    # Could add annotations or additional traces based on data
                    # For simplicity, we just note that the data is available.
                else:
                    print(f"⚠️ {name} endpoint returned {resp.status_code}")
            except requests.exceptions.RequestException:
                print(f"⚠️ {name} endpoint not available")

    fig.show()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Plot Pareto frontier from Green Agent API")
    parser.add_argument('--enhancements', action='store_true',
                        help='Include advanced enhancement metrics (MODP, RLHF, graph, distillation)')
    args = parser.parse_args()
    plot_pareto(include_enhancements=args.enhancements)
