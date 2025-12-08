"""
Earthquake Analytics Dashboard - Professional Edition
Clean, modern design with professional color scheme
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
from datetime import datetime, timedelta
import os
import requests

# =============================================================================
# PAGE CONFIG & THEME
# =============================================================================

st.set_page_config(
    page_title="Earthquake Analytics | Dashboard",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Professional Color Palette
COLORS = {
    'primary': '#0066CC',      # Professional Blue
    'secondary': '#00A3E0',    # Light Blue
    'accent': '#FF6B35',       # Orange accent
    'success': '#28A745',      # Green
    'warning': '#FFC107',      # Yellow
    'danger': '#DC3545',       # Red
    'dark': '#1E2A3A',         # Dark blue-gray
    'light': '#F8F9FA',        # Light gray
    'text': '#2C3E50',         # Dark text
    'muted': '#6C757D',        # Muted text
    'card_bg': '#FFFFFF',      # Card background
    'gradient_start': '#0066CC',
    'gradient_end': '#00A3E0',
}

# Professional CSS
st.markdown(f"""
<style>
    /* Import Google Font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Global Styles */
    .stApp {{
        font-family: 'Inter', sans-serif;
    }}
    
    /* Hide Streamlit branding */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    
    /* Main Header */
    .main-header {{
        font-size: 2.2rem;
        font-weight: 700;
        color: {COLORS['dark']};
        text-align: center;
        padding: 1.5rem 0;
        margin-bottom: 1rem;
        border-bottom: 3px solid {COLORS['primary']};
    }}
    
    .sub-header {{
        font-size: 1rem;
        color: {COLORS['muted']};
        text-align: center;
        margin-top: -1rem;
        margin-bottom: 2rem;
    }}
    
    /* Section Headers */
    .section-header {{
        font-size: 1.3rem;
        font-weight: 600;
        color: {COLORS['dark']};
        margin: 1.5rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid {COLORS['secondary']};
    }}
    
    /* KPI Cards */
    .kpi-container {{
        display: flex;
        justify-content: space-between;
        gap: 1rem;
        margin: 1rem 0;
    }}
    
    .kpi-card {{
        background: linear-gradient(135deg, {COLORS['primary']} 0%, {COLORS['secondary']} 100%);
        border-radius: 12px;
        padding: 1.5rem;
        text-align: center;
        box-shadow: 0 4px 20px rgba(0, 102, 204, 0.15);
        transition: transform 0.2s ease;
    }}
    
    .kpi-card:hover {{
        transform: translateY(-2px);
    }}
    
    .kpi-value {{
        font-size: 2rem;
        font-weight: 700;
        color: #FFFFFF;
        margin: 0;
        line-height: 1.2;
    }}
    
    .kpi-label {{
        font-size: 0.85rem;
        color: rgba(255,255,255,0.9);
        margin-top: 0.5rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }}
    
    /* Card Variants */
    .kpi-card-primary {{
        background: linear-gradient(135deg, #0066CC 0%, #0052A3 100%);
    }}
    .kpi-card-success {{
        background: linear-gradient(135deg, #28A745 0%, #1E7E34 100%);
    }}
    .kpi-card-warning {{
        background: linear-gradient(135deg, #FF6B35 0%, #E55A2B 100%);
    }}
    .kpi-card-danger {{
        background: linear-gradient(135deg, #DC3545 0%, #B02A37 100%);
    }}
    .kpi-card-info {{
        background: linear-gradient(135deg, #00A3E0 0%, #0088BE 100%);
    }}
    .kpi-card-dark {{
        background: linear-gradient(135deg, #1E2A3A 0%, #2C3E50 100%);
    }}
    
    /* Info Box */
    .info-box {{
        background: #E8F4FD;
        border-left: 4px solid {COLORS['primary']};
        padding: 1rem 1.5rem;
        border-radius: 0 8px 8px 0;
        margin: 1rem 0;
    }}
    
    .info-box-title {{
        font-weight: 600;
        color: {COLORS['dark']};
        margin-bottom: 0.5rem;
    }}
    
    .info-box-text {{
        color: {COLORS['text']};
        font-size: 0.9rem;
    }}
    
    /* Tabs Styling */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 8px;
        background-color: #F8F9FA;
        padding: 0.5rem;
        border-radius: 10px;
    }}
    
    .stTabs [data-baseweb="tab"] {{
        height: 45px;
        padding: 0 24px;
        background-color: transparent;
        border-radius: 8px;
        font-weight: 500;
        color: {COLORS['text']};
    }}
    
    .stTabs [aria-selected="true"] {{
        background-color: {COLORS['primary']} !important;
        color: white !important;
    }}
    
    /* Data Table */
    .dataframe {{
        font-size: 0.85rem;
    }}
    
    /* Metric styling */
    [data-testid="stMetricValue"] {{
        font-size: 1.8rem;
        font-weight: 600;
    }}
    
    /* Button styling */
    .stButton > button {{
        background: linear-gradient(135deg, {COLORS['primary']} 0%, {COLORS['secondary']} 100%);
        color: white;
        border: none;
        padding: 0.5rem 2rem;
        border-radius: 8px;
        font-weight: 500;
        transition: all 0.2s ease;
    }}
    
    .stButton > button:hover {{
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(0, 102, 204, 0.3);
    }}
    
    /* Divider */
    hr {{
        border: none;
        height: 1px;
        background: linear-gradient(90deg, transparent, {COLORS['secondary']}, transparent);
        margin: 2rem 0;
    }}
    
    /* Card Container */
    .chart-container {{
        background: white;
        border-radius: 12px;
        padding: 1rem;
        box-shadow: 0 2px 12px rgba(0,0,0,0.08);
        margin: 1rem 0;
    }}
</style>
""", unsafe_allow_html=True)

# Standard layout settings for charts (light charts on dark background)
CHART_LAYOUT = {
    'paper_bgcolor': 'rgba(255,255,255,1)',
    'plot_bgcolor': 'rgba(255,255,255,1)',
    'font': {'family': 'Inter, sans-serif', 'color': '#2C3E50'}
}


# =============================================================================
# DATABASE CONNECTION
# =============================================================================

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT', 'sfedu02-lvb17920'),
        user=os.getenv('SNOWFLAKE_USER', 'PLATYPUS'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'PLATYPUS_QUERY_WH'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'USER_DB_PLATYPUS'),
        schema=os.getenv('SNOWFLAKE_SCHEMA', 'ANALYTICS')
    )


def run_query(query):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0].lower() for desc in cursor.description]
    data = cursor.fetchall()
    return pd.DataFrame(data, columns=columns)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def render_kpi(value, label, card_type="primary"):
    return f"""
    <div class="kpi-card kpi-card-{card_type}">
        <p class="kpi-value">{value}</p>
        <p class="kpi-label">{label}</p>
    </div>
    """


def format_number(num):
    if num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K"
    return f"{num:,.0f}"


# =============================================================================
# DATA LOADING
# =============================================================================

@st.cache_data(ttl=60)
def load_recent_data(days=30, min_mag=0):
    query = f"""
    SELECT event_id, event_timestamp, event_date, magnitude, depth_km,
           latitude, longitude, region, place, significance,
           magnitude_category, depth_category, has_tsunami_warning
    FROM ANALYTICS.FCT_EARTHQUAKES
    WHERE event_date >= CURRENT_DATE - {days} AND magnitude >= {min_mag}
    ORDER BY event_timestamp DESC
    """
    return run_query(query)


@st.cache_data(ttl=300)
def load_stats():
    query = """
    SELECT COUNT(*) as total, MIN(event_date) as min_date, MAX(event_date) as max_date,
           MAX(magnitude) as max_mag, COUNT(DISTINCT YEAR(event_date)) as years,
           COUNT(CASE WHEN magnitude >= 7 THEN 1 END) as major_count
    FROM ANALYTICS.FCT_EARTHQUAKES
    """
    return run_query(query)


@st.cache_data(ttl=300)
def load_yearly():
    query = """
    SELECT YEAR(event_date) as year, COUNT(*) as count, AVG(magnitude) as avg_mag,
           MAX(magnitude) as max_mag,
           COUNT(CASE WHEN magnitude >= 5 THEN 1 END) as sig_5,
           COUNT(CASE WHEN magnitude >= 6 THEN 1 END) as sig_6,
           COUNT(CASE WHEN magnitude >= 7 THEN 1 END) as sig_7
    FROM ANALYTICS.FCT_EARTHQUAKES
    WHERE magnitude IS NOT NULL
    GROUP BY YEAR(event_date) ORDER BY year
    """
    return run_query(query)


@st.cache_data(ttl=300)
def load_monthly(start_year=2015):
    query = f"""
    SELECT YEAR(event_date) as year, MONTH(event_date) as month,
           COUNT(*) as count, AVG(magnitude) as avg_mag
    FROM ANALYTICS.FCT_EARTHQUAKES
    WHERE YEAR(event_date) >= {start_year}
    GROUP BY YEAR(event_date), MONTH(event_date)
    ORDER BY year, month
    """
    return run_query(query)


@st.cache_data(ttl=300)
def load_regions(limit=30):
    query = f"""
    SELECT region, COUNT(*) as count, AVG(magnitude) as avg_mag, MAX(magnitude) as max_mag
    FROM ANALYTICS.FCT_EARTHQUAKES
    WHERE region IS NOT NULL
    GROUP BY region ORDER BY count DESC LIMIT {limit}
    """
    return run_query(query)


@st.cache_data(ttl=300)
def load_risk():
    query = "SELECT * FROM ANALYTICS.AGG_REGIONAL_RISK ORDER BY risk_score DESC LIMIT 20"
    return run_query(query)


@st.cache_data(ttl=300)
def load_magnitude_dist():
    query = """
    SELECT magnitude_category, COUNT(*) as count
    FROM ANALYTICS.FCT_EARTHQUAKES
    WHERE magnitude_category IS NOT NULL
    GROUP BY magnitude_category
    """
    return run_query(query)


@st.cache_data(ttl=60)
def load_map_data(limit=5000):
    query = f"""
    SELECT latitude, longitude, magnitude, depth_km, place, event_date
    FROM ANALYTICS.FCT_EARTHQUAKES
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    ORDER BY event_date DESC LIMIT {limit}
    """
    return run_query(query)


# =============================================================================
# CHART FUNCTIONS
# =============================================================================

def create_yearly_chart(df):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(
        go.Bar(x=df['year'], y=df['count'], name='Earthquakes',
               marker_color=COLORS['primary'], opacity=0.8),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=df['year'], y=df['avg_mag'], name='Avg Magnitude',
                   line=dict(color=COLORS['accent'], width=3), mode='lines+markers'),
        secondary_y=True
    )
    
    fig.update_layout(
        title={'text': 'Annual Earthquake Activity', 'font': {'size': 16, 'color': '#2C3E50'}},
        height=420,
        hovermode='x unified',
        legend=dict(orientation='h', yanchor='bottom', y=1.02),
        margin=dict(t=80, b=40),
        paper_bgcolor='white',
        plot_bgcolor='#F8F9FA',
        font={'family': 'Inter, sans-serif', 'color': '#2C3E50'}
    )
    fig.update_xaxes(title='Year', gridcolor='rgba(0,0,0,0.1)', linecolor='#dee2e6')
    fig.update_yaxes(title='Count', secondary_y=False, gridcolor='rgba(0,0,0,0.1)', linecolor='#dee2e6')
    fig.update_yaxes(title='Magnitude', secondary_y=True, gridcolor='rgba(0,0,0,0.1)')
    
    return fig


def create_major_trend(df):
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['year'], y=df['sig_5'], name='M ≥ 5.0',
        fill='tozeroy', line=dict(color=COLORS['success'], width=2)
    ))
    fig.add_trace(go.Scatter(
        x=df['year'], y=df['sig_6'], name='M ≥ 6.0',
        fill='tozeroy', line=dict(color=COLORS['warning'], width=2)
    ))
    fig.add_trace(go.Scatter(
        x=df['year'], y=df['sig_7'], name='M ≥ 7.0',
        fill='tozeroy', line=dict(color=COLORS['danger'], width=2)
    ))
    
    fig.update_layout(
        title={'text': 'Significant Earthquakes Trend', 'font': {'size': 16, 'color': '#2C3E50'}},
        height=380,
        hovermode='x unified',
        legend=dict(orientation='h', yanchor='bottom', y=1.02),
        margin=dict(t=80, b=40),
        paper_bgcolor='white',
        plot_bgcolor='#F8F9FA',
        font={'family': 'Inter, sans-serif', 'color': '#2C3E50'}
    )
    fig.update_xaxes(gridcolor='rgba(0,0,0,0.1)', linecolor='#dee2e6')
    fig.update_yaxes(gridcolor='rgba(0,0,0,0.1)', linecolor='#dee2e6')
    
    return fig


def create_heatmap(df):
    df['year'] = df['year'].astype(int)
    df['month'] = df['month'].astype(int)
    pivot = df.pivot(index='year', columns='month', values='count')
    
    fig = px.imshow(
        pivot,
        labels=dict(x='Month', y='Year', color='Count'),
        color_continuous_scale='Blues',
        aspect='auto',
        title='Monthly Activity Heatmap'
    )
    fig.update_xaxes(
        tickvals=list(range(1, 13)),
        ticktext=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    )
    fig.update_layout(
        height=380,
        margin=dict(t=60, b=40),
        paper_bgcolor='white',
        font={'family': 'Inter, sans-serif', 'color': '#2C3E50'},
        title={'font': {'size': 16, 'color': '#2C3E50'}}
    )
    return fig


def create_map(df):
    # Filter out invalid magnitude values (must be positive for size)
    df_valid = df[df['magnitude'] > 0].copy()
    
    if len(df_valid) == 0:
        return go.Figure()
    
    fig = px.scatter_mapbox(
        df_valid, lat='latitude', lon='longitude',
        color='magnitude', size='magnitude',
        hover_name='place',
        hover_data={'magnitude': ':.1f', 'depth_km': ':.1f'},
        color_continuous_scale='YlOrRd',
        size_max=12, zoom=1,
        mapbox_style='carto-positron'
    )
    fig.update_layout(
        height=480,
        margin=dict(l=0, r=0, t=0, b=0),
        coloraxis_colorbar=dict(title='Magnitude')
    )
    return fig


def create_region_chart(df):
    fig = px.bar(
        df.head(15), x='count', y='region',
        orientation='h',
        color='avg_mag',
        color_continuous_scale='YlOrRd',
        text='count',
        title='Most Active Regions'
    )
    fig.update_layout(
        height=450,
        yaxis={'categoryorder': 'total ascending'},
        margin=dict(t=60, b=40, l=10),
        showlegend=False,
        paper_bgcolor='white',
        plot_bgcolor='#F8F9FA',
        font={'family': 'Inter, sans-serif', 'color': '#2C3E50'},
        title={'font': {'size': 16, 'color': '#2C3E50'}}
    )
    fig.update_traces(textposition='outside', textfont_size=10)
    fig.update_xaxes(gridcolor='rgba(0,0,0,0.1)')
    fig.update_yaxes(gridcolor='rgba(0,0,0,0.1)')
    return fig


def create_risk_chart(df):
    colors_map = {'high': COLORS['danger'], 'moderate': COLORS['warning'],
                  'low': COLORS['primary'], 'minimal': COLORS['success']}
    
    fig = px.bar(
        df.head(15), x='risk_score', y='region',
        orientation='h', color='risk_category',
        color_discrete_map=colors_map,
        title='Regional Risk Assessment'
    )
    fig.update_layout(
        height=450,
        yaxis={'categoryorder': 'total ascending'},
        margin=dict(t=60, b=40, l=10),
        paper_bgcolor='white',
        plot_bgcolor='#F8F9FA',
        font={'family': 'Inter, sans-serif', 'color': '#2C3E50'},
        title={'font': {'size': 16, 'color': '#2C3E50'}}
    )
    fig.update_xaxes(gridcolor='rgba(0,0,0,0.1)')
    fig.update_yaxes(gridcolor='rgba(0,0,0,0.1)')
    return fig


def create_magnitude_pie(df):
    order = ['micro', 'minor', 'light', 'moderate', 'strong', 'major', 'great']
    df['order'] = df['magnitude_category'].apply(lambda x: order.index(x) if x in order else 99)
    df = df.sort_values('order')
    
    colors = [COLORS['success'], COLORS['secondary'], COLORS['primary'],
              COLORS['warning'], COLORS['accent'], COLORS['danger'], '#8B0000']
    
    fig = px.pie(
        df, values='count', names='magnitude_category',
        color_discrete_sequence=colors,
        hole=0.45,
        title='Magnitude Distribution'
    )
    fig.update_layout(
        height=380,
        margin=dict(t=60, b=40),
        paper_bgcolor='white',
        font={'family': 'Inter, sans-serif', 'color': '#2C3E50'},
        title={'font': {'size': 16, 'color': '#2C3E50'}}
    )
    fig.update_traces(textposition='outside', textinfo='percent+label')
    return fig


# =============================================================================
# TAB 1: OVERVIEW
# =============================================================================

def render_overview():
    # Filters in columns
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        days = st.selectbox("Time Period", [7, 14, 30, 90, 180, 365], index=2, format_func=lambda x: f"Last {x} days")
    with col2:
        min_mag = st.selectbox("Min Magnitude", [0, 1, 2, 3, 4, 5], index=2, format_func=lambda x: f"≥ {x}.0")
    
    # Load data
    df = load_recent_data(days, min_mag)
    df_map = load_map_data(3000)
    df_risk = load_risk()
    stats = load_stats()
    
    # KPIs
    st.markdown("---")
    
    cols = st.columns(6)
    
    total = len(df)
    recent_24h = len(df[pd.to_datetime(df['event_timestamp']) >= (datetime.now() - timedelta(days=1))]) if len(df) > 0 else 0
    max_mag = df['magnitude'].max() if len(df) > 0 else 0
    avg_mag = df['magnitude'].mean() if len(df) > 0 else 0
    sig_count = len(df[df['magnitude'] >= 5.0]) if len(df) > 0 else 0
    tsunami = int(df['has_tsunami_warning'].sum()) if 'has_tsunami_warning' in df.columns and len(df) > 0 else 0
    
    with cols[0]:
        st.markdown(render_kpi(format_number(total), "Total Events", "primary"), unsafe_allow_html=True)
    with cols[1]:
        st.markdown(render_kpi(format_number(recent_24h), "Last 24 Hours", "info"), unsafe_allow_html=True)
    with cols[2]:
        st.markdown(render_kpi(f"{max_mag:.1f}", "Max Magnitude", "danger"), unsafe_allow_html=True)
    with cols[3]:
        st.markdown(render_kpi(f"{avg_mag:.2f}", "Avg Magnitude", "warning"), unsafe_allow_html=True)
    with cols[4]:
        st.markdown(render_kpi(format_number(sig_count), "Significant (≥5)", "dark"), unsafe_allow_html=True)
    with cols[5]:
        st.markdown(render_kpi(str(tsunami), "Tsunami Alerts", "success"), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Map
    st.markdown('<p class="section-header">🗺️ Global Earthquake Map</p>', unsafe_allow_html=True)
    if len(df_map) > 0:
        st.plotly_chart(create_map(df_map), use_container_width=True)
    
    st.markdown("---")
    
    # Charts row
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<p class="section-header">⚠️ Risk Assessment</p>', unsafe_allow_html=True)
        if len(df_risk) > 0:
            st.plotly_chart(create_risk_chart(df_risk), use_container_width=True)
    
    with col2:
        st.markdown('<p class="section-header">📊 Magnitude Breakdown</p>', unsafe_allow_html=True)
        df_mag = load_magnitude_dist()
        if len(df_mag) > 0:
            st.plotly_chart(create_magnitude_pie(df_mag), use_container_width=True)
    
    st.markdown("---")
    
    # Recent events table
    st.markdown('<p class="section-header">📋 Recent Seismic Events</p>', unsafe_allow_html=True)
    if len(df) > 0:
        display_df = df[['event_timestamp', 'place', 'magnitude', 'depth_km', 'region']].head(15)
        display_df.columns = ['Timestamp', 'Location', 'Magnitude', 'Depth (km)', 'Region']
        st.dataframe(display_df, use_container_width=True, hide_index=True)


# =============================================================================
# TAB 2: HISTORICAL ANALYSIS
# =============================================================================

def render_historical():
    stats = load_stats()
    
    # Stats bar
    if len(stats) > 0:
        cols = st.columns(6)
        with cols[0]:
            st.markdown(render_kpi(format_number(stats['total'].iloc[0]), "Total Records", "primary"), unsafe_allow_html=True)
        with cols[1]:
            st.markdown(render_kpi(str(stats['min_date'].iloc[0])[:10], "First Record", "info"), unsafe_allow_html=True)
        with cols[2]:
            st.markdown(render_kpi(str(stats['max_date'].iloc[0])[:10], "Latest Record", "success"), unsafe_allow_html=True)
        with cols[3]:
            st.markdown(render_kpi(str(stats['years'].iloc[0]), "Years Covered", "warning"), unsafe_allow_html=True)
        with cols[4]:
            st.markdown(render_kpi(f"{stats['max_mag'].iloc[0]:.1f}", "Max Recorded", "danger"), unsafe_allow_html=True)
        with cols[5]:
            st.markdown(render_kpi(format_number(stats['major_count'].iloc[0]), "Major (≥7.0)", "dark"), unsafe_allow_html=True)
    
    st.markdown("---")
    
    df_yearly = load_yearly()
    
    # Yearly trend
    st.markdown('<p class="section-header">📈 Annual Trends</p>', unsafe_allow_html=True)
    if len(df_yearly) > 0:
        st.plotly_chart(create_yearly_chart(df_yearly), use_container_width=True)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<p class="section-header">⚠️ Significant Events Trend</p>', unsafe_allow_html=True)
        if len(df_yearly) > 0:
            st.plotly_chart(create_major_trend(df_yearly), use_container_width=True)
    
    with col2:
        st.markdown('<p class="section-header">🗓️ Monthly Heatmap</p>', unsafe_allow_html=True)
        start_year = st.slider("Start Year", 1990, 2020, 2015, key="hm_year")
        df_monthly = load_monthly(start_year)
        if len(df_monthly) > 0:
            st.plotly_chart(create_heatmap(df_monthly), use_container_width=True)
    
    st.markdown("---")
    
    # Year comparison
    st.markdown('<p class="section-header">📊 Year-over-Year Comparison</p>', unsafe_allow_html=True)
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        years = sorted(df_yearly['year'].unique(), reverse=True)
        selected = st.multiselect("Select Years", years, default=list(years[:5]))
        metric = st.radio("Metric", ['count', 'avg_mag', 'max_mag', 'sig_5', 'sig_6', 'sig_7'])
    
    with col2:
        if selected:
            df_compare = df_yearly[df_yearly['year'].isin(selected)]
            fig = px.bar(df_compare, x='year', y=metric, color='year', text=metric,
                         title=f'Comparison: {metric}')
            fig.update_layout(
                height=350,
                showlegend=False,
                paper_bgcolor='white',
                plot_bgcolor='#F8F9FA',
                font={'family': 'Inter, sans-serif', 'color': '#2C3E50'},
                title={'font': {'size': 16, 'color': '#2C3E50'}}
            )
            fig.update_traces(texttemplate='%{text:.0f}', textposition='outside')
            fig.update_xaxes(gridcolor='rgba(0,0,0,0.1)')
            fig.update_yaxes(gridcolor='rgba(0,0,0,0.1)')
            st.plotly_chart(fig, use_container_width=True)


# =============================================================================
# TAB 3: EXPLORER
# =============================================================================

def render_explorer():
    st.markdown("""
    <div class="info-box">
        <p class="info-box-title">🔍 Data Explorer</p>
        <p class="info-box-text">Filter and explore earthquake data by year, region, magnitude, and depth. Download results as CSV.</p>
    </div>
    """, unsafe_allow_html=True)
    
    df_yearly = load_yearly()
    df_regions = load_regions(100)
    
    # Filters
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        years = ['All'] + sorted([int(y) for y in df_yearly['year'].unique()], reverse=True)
        year = st.selectbox("Year", years, key="exp_year")
    
    with col2:
        regions = ['All'] + list(df_regions['region'].unique())
        region = st.selectbox("Region", regions, key="exp_region")
    
    with col3:
        mag_range = st.slider("Magnitude", 0.0, 10.0, (2.0, 9.0), 0.5, key="exp_mag")
    
    with col4:
        depth_range = st.slider("Depth (km)", 0, 700, (0, 700), 10, key="exp_depth")
    
    # Build query
    conditions = [
        f"magnitude BETWEEN {mag_range[0]} AND {mag_range[1]}",
        f"depth_km BETWEEN {depth_range[0]} AND {depth_range[1]}",
        "latitude IS NOT NULL"
    ]
    
    if year != 'All':
        conditions.append(f"YEAR(event_date) = {year}")
    if region != 'All':
        conditions.append(f"region = '{region.replace(chr(39), chr(39)+chr(39))}'")
    
    query = f"""
    SELECT event_date, place, magnitude, depth_km, latitude, longitude, region
    FROM ANALYTICS.FCT_EARTHQUAKES
    WHERE {' AND '.join(conditions)}
    ORDER BY event_date DESC LIMIT 50000
    """
    
    df = run_query(query)
    
    st.markdown("---")
    
    # Results
    if len(df) > 0:
        cols = st.columns(4)
        with cols[0]:
            st.markdown(render_kpi(format_number(len(df)), "Results", "primary"), unsafe_allow_html=True)
        with cols[1]:
            st.markdown(render_kpi(f"{df['magnitude'].max():.1f}", "Max Mag", "danger"), unsafe_allow_html=True)
        with cols[2]:
            st.markdown(render_kpi(f"{df['magnitude'].mean():.2f}", "Avg Mag", "warning"), unsafe_allow_html=True)
        with cols[3]:
            st.markdown(render_kpi(f"{df['depth_km'].mean():.0f}", "Avg Depth", "info"), unsafe_allow_html=True)
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Filter positive magnitudes for map
            df_map_valid = df[df['magnitude'] > 0].head(3000)
            if len(df_map_valid) > 0:
                fig = px.scatter_mapbox(
                    df_map_valid, lat='latitude', lon='longitude',
                    color='magnitude', size='magnitude',
                    color_continuous_scale='YlOrRd',
                    size_max=12, zoom=1,
                    mapbox_style='carto-positron',
                    hover_name='place'
                )
                fig.update_layout(height=400, margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.histogram(df, x='magnitude', nbins=30, color_discrete_sequence=[COLORS['primary']],
                               title='Magnitude Distribution')
            fig.update_layout(
                height=400,
                paper_bgcolor='white',
                plot_bgcolor='#F8F9FA',
                font={'family': 'Inter, sans-serif', 'color': '#2C3E50'},
                title={'font': {'size': 16, 'color': '#2C3E50'}}
            )
            fig.update_xaxes(gridcolor='rgba(0,0,0,0.1)')
            fig.update_yaxes(gridcolor='rgba(0,0,0,0.1)')
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        
        # Table with pagination
        st.markdown('<p class="section-header">📋 Results Table</p>', unsafe_allow_html=True)
        
        page_size = 25
        total_pages = max(1, len(df) // page_size + (1 if len(df) % page_size else 0))
        page = st.number_input("Page", 1, total_pages, 1)
        
        start = (page - 1) * page_size
        st.dataframe(df.iloc[start:start + page_size], use_container_width=True, hide_index=True)
        st.caption(f"Showing {start + 1}-{min(start + page_size, len(df))} of {len(df):,}")
        
        # Download
        st.download_button(
            "📥 Download CSV",
            df.to_csv(index=False),
            f"earthquakes_{datetime.now().strftime('%Y%m%d')}.csv",
            "text/csv"
        )
    else:
        st.warning("No data found. Adjust filters.")


# =============================================================================
# TAB 4: REGION ANALYSIS
# =============================================================================

def render_region():
    st.markdown("""
    <div class="info-box">
        <p class="info-box-title">🌍 Regional Deep Dive</p>
        <p class="info-box-text">Select a region to analyze its seismic activity patterns over time.</p>
    </div>
    """, unsafe_allow_html=True)
    
    df_regions = load_regions(50)
    
    col1, col2 = st.columns([2, 1])
    with col1:
        region = st.selectbox("Select Region", df_regions['region'].tolist())
    with col2:
        year_range = st.slider("Years", 1900, 2025, (2000, 2025))
    
    if region:
        query = f"""
        SELECT event_date, magnitude, depth_km, latitude, longitude, place,
               YEAR(event_date) as year, MONTH(event_date) as month
        FROM ANALYTICS.FCT_EARTHQUAKES
        WHERE region = '{region.replace(chr(39), chr(39)+chr(39))}'
          AND YEAR(event_date) BETWEEN {year_range[0]} AND {year_range[1]}
        ORDER BY event_date DESC
        """
        df = run_query(query)
        
        if len(df) > 0:
            st.markdown("---")
            
            cols = st.columns(5)
            with cols[0]:
                st.markdown(render_kpi(format_number(len(df)), "Total", "primary"), unsafe_allow_html=True)
            with cols[1]:
                st.markdown(render_kpi(f"{df['magnitude'].max():.1f}", "Max Mag", "danger"), unsafe_allow_html=True)
            with cols[2]:
                st.markdown(render_kpi(f"{df['magnitude'].mean():.2f}", "Avg Mag", "warning"), unsafe_allow_html=True)
            with cols[3]:
                st.markdown(render_kpi(f"{df['depth_km'].mean():.0f} km", "Avg Depth", "info"), unsafe_allow_html=True)
            with cols[4]:
                st.markdown(render_kpi(str(df['year'].nunique()), "Active Years", "success"), unsafe_allow_html=True)
            
            st.markdown("---")
            
            # Yearly trend for region
            yearly = df.groupby('year').agg({'magnitude': ['count', 'mean', 'max']}).reset_index()
            yearly.columns = ['year', 'count', 'avg', 'max']
            
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(x=yearly['year'], y=yearly['count'], name='Count', marker_color=COLORS['primary']), secondary_y=False)
            fig.add_trace(go.Scatter(x=yearly['year'], y=yearly['avg'], name='Avg Mag', line=dict(color=COLORS['accent'], width=3)), secondary_y=True)
            fig.update_layout(
                title={'text': f'Annual Activity: {region}', 'font': {'size': 16, 'color': '#2C3E50'}},
                height=380,
                paper_bgcolor='white',
                plot_bgcolor='#F8F9FA',
                font={'family': 'Inter, sans-serif', 'color': '#2C3E50'}
            )
            fig.update_xaxes(gridcolor='rgba(0,0,0,0.1)')
            fig.update_yaxes(gridcolor='rgba(0,0,0,0.1)')
            st.plotly_chart(fig, use_container_width=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Filter positive magnitudes for map
                df_map_valid = df[df['magnitude'] > 0].head(2000)
                if len(df_map_valid) > 0:
                    fig = px.scatter_mapbox(
                        df_map_valid, lat='latitude', lon='longitude',
                        color='magnitude', size='magnitude',
                        color_continuous_scale='YlOrRd',
                        zoom=3, mapbox_style='carto-positron'
                    )
                    fig.update_layout(height=350, margin=dict(l=0, r=0, t=0, b=0))
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.histogram(df, x='magnitude', nbins=25, color_discrete_sequence=[COLORS['primary']],
                                   title='Magnitude Distribution')
                fig.update_layout(
                    height=350,
                    paper_bgcolor='white',
                    plot_bgcolor='#F8F9FA',
                    font={'family': 'Inter, sans-serif', 'color': '#2C3E50'},
                    title={'font': {'size': 16, 'color': '#2C3E50'}}
                )
                fig.update_xaxes(gridcolor='rgba(0,0,0,0.1)')
                fig.update_yaxes(gridcolor='rgba(0,0,0,0.1)')
                st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("---")
            st.markdown(f'<p class="section-header">🔥 Largest Events in {region}</p>', unsafe_allow_html=True)
            st.dataframe(df.nlargest(10, 'magnitude')[['event_date', 'place', 'magnitude', 'depth_km']], hide_index=True)
        else:
            st.warning("No data for selected region and time range.")


# =============================================================================
# TAB 5: PIPELINE
# =============================================================================

def render_pipeline():
    st.markdown("""
    <div class="info-box">
        <p class="info-box-title">🚀 Data Pipeline Management</p>
        <p class="info-box-text">Trigger Airflow DAGs to load new earthquake data or run transformations.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<p class="section-header">⭐ Full Historical Load</p>', unsafe_allow_html=True)
        st.caption("Auto-chunks to handle API limits. Uses MERGE (safe to re-run).")
        
        f_start = st.date_input("Start Date", datetime(2020, 1, 1), key="f_start")
        f_end = st.date_input("End Date", datetime.now(), key="f_end")
        f_mag = st.number_input("Min Magnitude", 0.0, 9.0, 2.5, 0.5, key="f_mag")
        f_chunk = st.number_input("Chunk Days", 7, 365, 30, key="f_chunk")
        
        if st.button("🚀 Run Full Load", type="primary"):
            trigger_dag('earthquake_historical_full', f_start, f_end, f_mag, int(f_chunk))
    
    with col2:
        st.markdown('<p class="section-header">⚡ Quick Load</p>', unsafe_allow_html=True)
        st.caption("Single API call. Use for small date ranges.")
        
        q_start = st.date_input("Start Date", datetime.now() - timedelta(7), key="q_start")
        q_end = st.date_input("End Date", datetime.now(), key="q_end")
        q_mag = st.number_input("Min Magnitude", 0.0, 9.0, 2.5, 0.5, key="q_mag")
        
        c1, c2 = st.columns(2)
        with c1:
            if st.button("⚡ Bulk Insert"):
                trigger_dag('earthquake_historical_bulk', q_start, q_end, q_mag)
        with c2:
            if st.button("🔄 Merge"):
                trigger_dag('earthquake_historical_merge', q_start, q_end, q_mag)
    
    st.markdown("---")
    
    st.markdown('<p class="section-header">🔧 dbt Transformations</p>', unsafe_allow_html=True)
    st.caption("Run after loading data to update analytics tables.")
    
    if st.button("⚙️ Run dbt", type="secondary"):
        trigger_dag('earthquake_dbt_transform')
    
    st.markdown("---")
    
    # Current stats
    stats = load_stats()
    if len(stats) > 0:
        st.markdown('<p class="section-header">📊 Current Database Status</p>', unsafe_allow_html=True)
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Records", f"{stats['total'].iloc[0]:,}")
        col2.metric("Date Range", f"{str(stats['min_date'].iloc[0])[:10]} → {str(stats['max_date'].iloc[0])[:10]}")
        col3.metric("Years", stats['years'].iloc[0])


def trigger_dag(dag_id, start=None, end=None, mag=None, chunk=None):
    conf = {}
    if start: conf['start_date'] = start.strftime('%Y-%m-%d')
    if end: conf['end_date'] = end.strftime('%Y-%m-%d')
    if mag is not None: conf['min_magnitude'] = mag
    if chunk: conf['chunk_days'] = chunk
    
    try:
        r = requests.post(
            f"http://localhost:8081/api/v1/dags/{dag_id}/dagRuns",
            json={"conf": conf},
            auth=('airflow', 'airflow'),
            timeout=10
        )
        if r.status_code in [200, 201]:
            st.success(f"✅ Triggered {dag_id}")
        else:
            st.info(f"Go to [Airflow](http://localhost:8081) and trigger `{dag_id}` with config: `{conf}`")
    except:
        st.info(f"Go to [Airflow](http://localhost:8081) and trigger `{dag_id}` with config: `{conf}`")


# =============================================================================
# MAIN
# =============================================================================

def main():
    # Header
    st.markdown('<h1 class="main-header">🌐 Earthquake Analytics Platform</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Real-time seismic monitoring and historical analysis</p>', unsafe_allow_html=True)
    
    # Tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Overview",
        "📈 Historical",
        "🔍 Explorer",
        "🌍 Regions",
        "🚀 Pipeline"
    ])
    
    with tab1:
        render_overview()
    with tab2:
        render_historical()
    with tab3:
        render_explorer()
    with tab4:
        render_region()
    with tab5:
        render_pipeline()
    
    # Footer
    st.markdown("---")
    st.markdown(f"""
    <div style='text-align: center; padding: 1rem;'>
        <small style='color: {COLORS["muted"]};'>
            Data: USGS Earthquake Hazards Program | 
            Updated: {datetime.now().strftime("%Y-%m-%d %H:%M")} |
            Built with Streamlit & Snowflake
        </small>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
