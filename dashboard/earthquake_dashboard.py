"""
Earthquake Analytics Dashboard
Real-time interactive visualization of USGS earthquake data
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
from datetime import datetime, timedelta
import os

# Page configuration
st.set_page_config(
    page_title="🌍 Earthquake Analytics Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 1rem;
        text-align: center;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_snowflake_connection():
    """Create Snowflake connection"""
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT', 'sfedu02-lvb17920'),
        user=os.getenv('SNOWFLAKE_USER', 'PLATYPUS'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'PLATYPUS_QUERY_WH'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'USER_DB_PLATYPUS'),
        schema=os.getenv('SNOWFLAKE_SCHEMA', 'ANALYTICS')
    )


@st.cache_data(ttl=60)  # Cache for 60 seconds for near real-time
def load_earthquakes(days=30, min_magnitude=0):
    """Load earthquake data from Snowflake"""
    conn = get_snowflake_connection()
    query = f"""
    SELECT 
        event_id, event_timestamp, event_date, 
        magnitude, magnitude_category, depth_km, depth_category,
        latitude, longitude, region, place,
        felt_reports, significance, has_tsunami_warning,
        alert_level, source_network, status
    FROM ANALYTICS.fct_earthquakes
    WHERE event_date >= CURRENT_DATE - {days}
      AND magnitude >= {min_magnitude}
    ORDER BY event_timestamp DESC
    """
    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=60)
def load_daily_summary(days=30):
    """Load daily summary data"""
    conn = get_snowflake_connection()
    query = f"""
    SELECT *
    FROM ANALYTICS.agg_daily_summary
    WHERE summary_date >= CURRENT_DATE - {days}
    ORDER BY summary_date DESC
    """
    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_regional_risk():
    """Load regional risk data"""
    conn = get_snowflake_connection()
    query = """
    SELECT *
    FROM ANALYTICS.agg_regional_risk
    ORDER BY risk_score DESC
    LIMIT 50
    """
    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300)
def load_hourly_heatmap():
    """Load hourly heatmap data"""
    conn = get_snowflake_connection()
    query = """
    SELECT *
    FROM ANALYTICS.agg_hourly_heatmap
    ORDER BY day_of_week, event_hour
    """
    df = pd.read_sql(query, conn)
    return df


def create_map(df):
    """Create interactive earthquake map"""
    # Color scale based on magnitude
    fig = px.scatter_mapbox(
        df,
        lat='latitude',
        lon='longitude',
        color='magnitude',
        size='magnitude',
        hover_name='place',
        hover_data={
            'magnitude': ':.1f',
            'depth_km': ':.1f',
            'event_timestamp': True,
            'latitude': ':.3f',
            'longitude': ':.3f'
        },
        color_continuous_scale='YlOrRd',
        size_max=20,
        zoom=1,
        mapbox_style='carto-positron',
        title='🗺️ Earthquake Locations'
    )
    fig.update_layout(
        height=500,
        margin=dict(l=0, r=0, t=40, b=0)
    )
    return fig


def create_magnitude_histogram(df):
    """Create magnitude distribution histogram"""
    fig = px.histogram(
        df,
        x='magnitude',
        nbins=30,
        color='magnitude_category',
        title='📊 Magnitude Distribution',
        labels={'magnitude': 'Magnitude', 'count': 'Count'},
        color_discrete_map={
            'micro': '#90EE90',
            'minor': '#98FB98',
            'light': '#FFFF00',
            'moderate': '#FFA500',
            'strong': '#FF6347',
            'major': '#FF0000',
            'great': '#8B0000'
        }
    )
    fig.update_layout(height=350)
    return fig


def create_timeline(df_daily):
    """Create earthquake frequency timeline"""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(
        go.Bar(
            x=df_daily['summary_date'],
            y=df_daily['total_earthquakes'],
            name='Total Earthquakes',
            marker_color='lightblue'
        ),
        secondary_y=False
    )
    
    fig.add_trace(
        go.Scatter(
            x=df_daily['summary_date'],
            y=df_daily['avg_magnitude'],
            name='Avg Magnitude',
            line=dict(color='red', width=2),
            mode='lines+markers'
        ),
        secondary_y=True
    )
    
    fig.update_layout(
        title='📈 Earthquake Activity Over Time',
        height=350,
        hovermode='x unified'
    )
    fig.update_xaxes(title_text='Date')
    fig.update_yaxes(title_text='Count', secondary_y=False)
    fig.update_yaxes(title_text='Avg Magnitude', secondary_y=True)
    
    return fig


def create_depth_scatter(df):
    """Create depth vs magnitude scatter plot"""
    fig = px.scatter(
        df,
        x='depth_km',
        y='magnitude',
        color='depth_category',
        size='significance',
        hover_name='place',
        title='🔬 Depth vs Magnitude Analysis',
        labels={'depth_km': 'Depth (km)', 'magnitude': 'Magnitude'},
        color_discrete_map={
            'shallow': '#3498db',
            'intermediate': '#f39c12',
            'deep': '#e74c3c'
        }
    )
    fig.update_layout(height=350)
    return fig


def create_risk_bar(df_risk):
    """Create regional risk bar chart"""
    df_top = df_risk.head(15)
    
    fig = px.bar(
        df_top,
        x='risk_score',
        y='region',
        orientation='h',
        color='risk_category',
        title='⚠️ Top 15 High-Risk Regions',
        color_discrete_map={
            'high': '#e74c3c',
            'moderate': '#f39c12',
            'low': '#3498db',
            'minimal': '#2ecc71'
        }
    )
    fig.update_layout(
        height=400,
        yaxis={'categoryorder': 'total ascending'}
    )
    return fig


def create_heatmap(df_heatmap):
    """Create day/hour heatmap"""
    # Pivot the data for heatmap
    pivot = df_heatmap.pivot(
        index='day_name', 
        columns='event_hour', 
        values='earthquake_count'
    )
    
    # Reorder days
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    pivot = pivot.reindex(day_order)
    
    fig = px.imshow(
        pivot,
        labels=dict(x='Hour of Day', y='Day of Week', color='Count'),
        title='🕐 Earthquake Frequency by Day & Hour',
        color_continuous_scale='Viridis',
        aspect='auto'
    )
    fig.update_layout(height=300)
    return fig


# =============================================================================
# MAIN DASHBOARD
# =============================================================================

def main():
    # Header
    st.markdown('<h1 class="main-header">🌍 Real-Time Earthquake Analytics Dashboard</h1>', 
                unsafe_allow_html=True)
    st.markdown("---")
    
    # Sidebar filters
    st.sidebar.header("🔧 Filters")
    
    days_filter = st.sidebar.slider(
        "Days of data",
        min_value=1,
        max_value=365,
        value=30,
        help="Number of days of historical data to display"
    )
    
    min_mag_filter = st.sidebar.slider(
        "Minimum Magnitude",
        min_value=0.0,
        max_value=7.0,
        value=2.0,
        step=0.5,
        help="Filter earthquakes by minimum magnitude"
    )
    
    # Auto-refresh option
    auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh (60s)", value=False)
    if auto_refresh:
        st.sidebar.info("Dashboard refreshes every 60 seconds")
    
    # Load data
    with st.spinner("Loading earthquake data..."):
        try:
            df = load_earthquakes(days=days_filter, min_magnitude=min_mag_filter)
            df_daily = load_daily_summary(days=days_filter)
            df_risk = load_regional_risk()
            df_heatmap = load_hourly_heatmap()
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            st.info("Make sure your Snowflake connection is configured and dbt models have been run.")
            st.code("""
# Set environment variables:
export SNOWFLAKE_PASSWORD='your_password'
export SNOWFLAKE_ACCOUNT='sfedu02-lvb17920'
export SNOWFLAKE_USER='PLATYPUS'
            """)
            return
    
    # KPI Metrics Row
    st.subheader("📊 Key Metrics")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            label="Total Earthquakes",
            value=f"{len(df):,}",
            delta=f"Last {days_filter} days"
        )
    
    with col2:
        recent_count = len(df[df['event_timestamp'] >= (datetime.now() - timedelta(days=1))])
        st.metric(
            label="Last 24 Hours",
            value=f"{recent_count:,}",
            delta="events"
        )
    
    with col3:
        max_mag = df['magnitude'].max() if len(df) > 0 else 0
        st.metric(
            label="Max Magnitude",
            value=f"{max_mag:.1f}",
            delta="Richter"
        )
    
    with col4:
        avg_mag = df['magnitude'].mean() if len(df) > 0 else 0
        st.metric(
            label="Avg Magnitude",
            value=f"{avg_mag:.2f}",
            delta="Richter"
        )
    
    with col5:
        significant = len(df[df['magnitude'] >= 5.0])
        st.metric(
            label="Significant (≥5.0)",
            value=f"{significant:,}",
            delta="events"
        )
    
    st.markdown("---")
    
    # Main Map
    st.subheader("🗺️ Earthquake Map")
    if len(df) > 0:
        fig_map = create_map(df)
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.warning("No earthquake data available for the selected filters.")
    
    st.markdown("---")
    
    # Charts Row 1
    col1, col2 = st.columns(2)
    
    with col1:
        if len(df_daily) > 0:
            fig_timeline = create_timeline(df_daily)
            st.plotly_chart(fig_timeline, use_container_width=True)
    
    with col2:
        if len(df) > 0:
            fig_hist = create_magnitude_histogram(df)
            st.plotly_chart(fig_hist, use_container_width=True)
    
    st.markdown("---")
    
    # Charts Row 2
    col1, col2 = st.columns(2)
    
    with col1:
        if len(df) > 0:
            fig_depth = create_depth_scatter(df)
            st.plotly_chart(fig_depth, use_container_width=True)
    
    with col2:
        if len(df_risk) > 0:
            fig_risk = create_risk_bar(df_risk)
            st.plotly_chart(fig_risk, use_container_width=True)
    
    st.markdown("---")
    
    # Heatmap
    st.subheader("🕐 Temporal Patterns")
    if len(df_heatmap) > 0:
        fig_heatmap = create_heatmap(df_heatmap)
        st.plotly_chart(fig_heatmap, use_container_width=True)
    
    st.markdown("---")
    
    # Recent Earthquakes Table
    st.subheader("📋 Recent Earthquakes")
    
    if len(df) > 0:
        display_df = df[['event_timestamp', 'place', 'magnitude', 'depth_km', 
                         'magnitude_category', 'region', 'has_tsunami_warning']].head(20)
        display_df.columns = ['Time', 'Location', 'Magnitude', 'Depth (km)', 
                              'Category', 'Region', 'Tsunami Warning']
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: gray;'>
        <small>Data source: USGS Earthquake Hazards Program | 
        Last updated: {}</small>
    </div>
    """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")), 
    unsafe_allow_html=True)
    
    # Auto-refresh using rerun
    if auto_refresh:
        import time
        time.sleep(60)
        st.rerun()


if __name__ == "__main__":
    main()
