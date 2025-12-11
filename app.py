# fire_risk_dashboard.py
# -----------------------------------------------------------
# Fire Risk Analysis Dashboard
# -----------------------------------------------------------

import streamlit as st
import pandas as pd
import duckdb
import altair as alt

# ---------------- CONFIG ----------------
st.set_page_config(
    page_title="UK Fire Data Analysis",
    page_icon="🔥",
    layout="wide"
)

# --- DATABASE CONNECTION ---
@st.cache_data  # This caches the data so it runs fast
def load_data(query):
    """Connects to DuckDB, executes a query, and returns a Pandas DataFrame."""
    with duckdb.connect('dwelling_fires.duckdb') as con:
        df = con.execute(query).df()
    return df

# --- SQL QUERIES ---

# Query 1a: Vehicles by Dwelling Type
sql_vehicles = """
SELECT 
    d.dwelling_type,
    COUNT(f.fact_dwelling_fire_id) AS number_of_incidents,
    ROUND(AVG(dv.vehicles_midpoint), 2) AS avg_vehicles
FROM 
    fact_dwelling_fire f
JOIN 
    dim_dwelling d ON f.dwelling_key = d.dwelling_key
JOIN 
    dim_vehicles dv ON f.vehicles_key = dv.vehicles_key
WHERE 
    dv.vehicles_midpoint IS NOT NULL
    AND d.dwelling_type IS NOT NULL
GROUP BY 
    d.dwelling_type
ORDER BY 
    avg_vehicles DESC
"""

# Query 1b: Fire Start Locations
sql_locations = """
SELECT 
    d.dwelling_type,
    dl.fire_start_location,
    COUNT(f.fact_dwelling_fire_id) AS number_of_incidents
FROM 
    fact_dwelling_fire f
JOIN 
    dim_dwelling d ON f.dwelling_key = d.dwelling_key
JOIN 
    dim_location dl ON f.location_key = dl.location_key
WHERE 
    dl.fire_start_location IS NOT NULL
    AND dl.fire_start_location != 'Not known'
    AND d.dwelling_type IS NOT NULL
GROUP BY 
    d.dwelling_type,
    dl.fire_start_location
"""

# Query 2: The Human Cost & Destruction Analysis
sql_human_cost = """
SELECT 
    di.cause_of_fire,
    COUNT(f.fact_dwelling_fire_id) AS number_of_incidents,
    ROUND(AVG(ds.spread_rank), 2) AS avg_spread_rank,
    SUM(f.fatality_casualty_flag) AS total_incidents_with_casualties,
    ROUND(AVG(f.fatality_casualty_flag) * 100, 2) AS pct_chance_of_casualty,
    ROUND(AVG(f.rescues), 2) AS avg_rescues_per_incident
FROM 
    fact_dwelling_fire f
JOIN 
    dim_ignition di ON f.ignition_key = di.ignition_key
JOIN 
    dim_spread ds ON f.spread_key = ds.spread_key
WHERE 
    ds.spread_rank IS NOT NULL
    AND f.rescues IS NOT NULL
    AND di.cause_of_fire IS NOT NULL
    AND di.cause_of_fire != 'Not known'
GROUP BY 
    di.cause_of_fire
HAVING
    COUNT(f.fact_dwelling_fire_id) > 100 -- Filter out noise
"""

# Query 3: The SQL code itself (for display in Tab 4)
sql_to_display = """
-- Final 'Human Cost & Complexity' query
SELECT 
    di.cause_of_fire,
    COUNT(*) AS num_incidents,
    ROUND(AVG(ds.spread_rank), 2) AS avg_spread_rank,
    ROUND(AVG(f.fatality_casualty_flag) * 100, 2) AS pct_chance_of_casualty,
    ROUND(AVG(f.rescues), 2) AS avg_rescues
FROM 
    fact_dwelling_fire f
JOIN 
    dim_ignition di ON f.ignition_key = di.ignition_key
JOIN 
    dim_spread ds ON f.spread_key = ds.spread_key
WHERE 
    ds.spread_rank IS NOT NULL
    AND f.rescues IS NOT NULL
    AND di.cause_of_fire IS NOT NULL
GROUP BY 
    di.cause_of_fire
HAVING
    COUNT(f.fact_dwelling_fire_id) > 100
"""

# --- PANDAS PROCESSING for 'House vs. Flats' Chart ---
@st.cache_data
def process_location_data():
    df_locations_raw = load_data(sql_locations)
    
    def group_buildings(dwelling_type):
        if dwelling_type == 'House - single occupancy':
            return 'House'
        if dwelling_type.startswith('Purpose Built'):
            return 'Purpose Built Flats'
        return 'Other'

    def group_locations(location):
        if location == 'Kitchen':
            return 'Kitchen'
        if location in ('Living Room', 'Bedroom/ Bedsitting Room'):
            return 'Living/Bedroom'
        if location in ('Refuse Store', 'Corridor/ Hall/ Open Plan Area/ Reception Area', 'Stairs/ Under stairs (enclosed area)'):
            return 'Communal/Escape Routes'
        return 'Other Room/External'

    df_locations_raw['building_group'] = df_locations_raw['dwelling_type'].apply(group_buildings)
    df_locations_raw['location_group'] = df_locations_raw['fire_start_location'].apply(group_locations)

    df_groups = df_locations_raw[
        df_locations_raw['building_group'].isin(['House', 'Purpose Built Flats'])
    ].copy() 

    df_grouped_counts = df_groups.groupby(
        ['building_group', 'location_group']
    )['number_of_incidents'].sum().reset_index()

    group_totals = df_grouped_counts.groupby('building_group')['number_of_incidents'].sum()
    df_grouped_counts['group_total'] = df_grouped_counts['building_group'].map(group_totals)
    df_grouped_counts['percentage'] = (
        100 * df_grouped_counts['number_of_incidents'] / df_grouped_counts['group_total']
    ).round(1)

    df_locations_pivot_pct = df_grouped_counts.pivot(
        index='location_group', 
        columns='building_group', 
        values='percentage'
    ).fillna(0)

    df_locations_pivot_pct = df_locations_pivot_pct.drop('Other Room/External', errors='ignore')
    
    # Un-pivot the data for Altair
    df_locations_chart_data = df_locations_pivot_pct.reset_index().melt(
        id_vars='location_group', 
        var_name='building_group',
        value_name='percentage'
    )
    
    return df_locations_chart_data, df_locations_pivot_pct

# ---------------- SIDEBAR NAVIGATION ----------------
st.sidebar.title("🚒 Fire Risk Dashboard")
page = st.sidebar.radio(
    "Navigate to:",
    [
        "Overview",
        "Response vs Risk",
        "Human vs. Property Risk",
        "Conclusions", 
        "Methodology & ETL" 
    ]
)

# ---------------- PAGE 1: OVERVIEW ----------------
if page == "Overview":
    st.title("🔍 Fire Risk Analysis Dashboard")
    st.markdown("""
    Welcome to the **UK Fire Incident Risk Analysis**, a project exploring national fire data to uncover:
    - Patterns of **incident response behaviour**
    - The **human and property cost** of fire types
    - Insights for **prevention and policy** planning

    ---

    ### 🧩 Dataset Overview
    - Over **450,000** dwelling fire incidents (2010–2025)
    - Attributes: Date, Cause, Property Type, Casualties, Fire Spread, Response Times & Resources.
    - Source: [Home Office Fire Statistics – Incident Level Data](https://www.gov.uk/government/statistics/fire-statistics-incident-level-datasets)

    ---

    ### 🧮 Analytical Themes
    1. **Response by Building Risk** – How mobilisation varies by property type.
    2. **Human vs Property Risk** – Contrasting casualty likelihood vs. property destruction.
    3. **Operational Complexity** – Uncovering patterns in rescues and severe fire spread.

    This analysis explores *how* and *why* the fire service response changes based on the type of dwelling.
    The data shows that response intensity isn’t driven purely by incident volume—it’s driven by **risk**.
    """)

    # --- Load Data ---
    df_vehicles = load_data(sql_vehicles)
    df_locations_chart_data, df_locations_pivot_pct = process_location_data()
    df_vehicles_top5 = df_vehicles.head(5)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 5 by Average Vehicle Response")
        
        with st.expander("Read Analysis", expanded=False):
            st.markdown("""
            This chart clearly shows a tiered response system. A standard house receives a benchmark response of ~2.6 vehicles. 
            
            This dramatically increases for medium and high-rise flats, proving the service pre-plans for the added complexity and risk of taller buildings.
            """)
        
        # --- Chart 1: Altair Vertical Column Chart (Labels Fixed & Added) ---
        base = alt.Chart(df_vehicles_top5).encode(
            x=alt.X("dwelling_type", 
                    sort=None, 
                    title="Dwelling Type",
                    axis=alt.Axis(labelAngle=-45)), # Angled labels
            y=alt.Y("avg_vehicles", 
                    title="Average Vehicles per Incident"),
            tooltip=["dwelling_type", "number_of_incidents", "avg_vehicles"]
        )
        
        bars = base.mark_bar()
        
        text = base.mark_text(
            align='center',
            baseline='bottom',
            dy=-5  
        ).encode(
            text=alt.Text("avg_vehicles", format=".2f"),
            color=alt.value("black")
        )

        chart1 = (bars + text).interactive()
        
        st.altair_chart(chart1, use_container_width=True)
        
        with st.expander("View Full Data Table (All Dwelling Types)"):
            st.dataframe(df_vehicles)

    with col2:
        st.subheader("Where Do Fires Start? (House vs. Flats)")
        
        with st.expander("Read Analysis", expanded=False):
            st.markdown("""
            This chart shows *why* the risk profile is different. It compares the percentage of fires in key locations for a 'House' vs. all 'Purpose Built Flats'.
            
            **'Purpose Built Flats' introduce new, high-risk fire locations** that don't exist in a standard house. 
            
            The **'Communal/Escape Routes'** group (corridors, stairs, refuse stores) makes up **over 10%** of all flat fires, but is a tiny fraction of house fires. This different risk profile, which threatens the escape of all residents, is a key justification for the larger initial response.
            """)

        # --- Chart 2: **FIXED** True Clustered Column Chart (using xOffset) ---
        
        # 1. Base chart definition
        base = alt.Chart(df_locations_chart_data).encode(
            # X-axis is the location group
            x=alt.X('location_group', title='Fire Start Location', axis=alt.Axis(labelAngle=-45)),
            
            # Y-axis is the percentage
            y=alt.Y('percentage', title='Percentage of Incidents'),
            
            # Color is by building group
            color=alt.Color('building_group', title='Building Group'),
            
            # This is the key: it "dodges" the bars side-by-side
            xOffset=alt.XOffset('building_group'),
            
            tooltip=['location_group', 'building_group', 'percentage']
        )
        
        # 2. Bars - using a fixed width for spacing
        bars = base.mark_bar(width=30)
        
        # 3. Text labels
        text = base.mark_text(
            align='center',
            baseline='bottom',
            dy=-5
        ).encode(
            text=alt.Text('percentage', format=".1f"),
            color=alt.value('black')
        )
        
        # 4. Layer and make interactive
        chart2 = (bars + text).interactive()
        
        st.altair_chart(chart2, use_container_width=True)
        st.caption("Data: Percentage of all incidents for each building type.")

        # --- **FIX**: Added the missing data table ---
        with st.expander("View Full Data Table"):
            st.dataframe(df_locations_pivot_pct)


# ---------------- PAGE 3: ANALYSIS 2 ----------------
elif page == "Human vs. Property Risk":
    st.title("🔥 Analysis: Human vs. Property Risk")
    st.markdown("""
    This section contrasts **human casualty risk** with **property destruction and complexity**, revealing that
    *the causes of fire that endanger people are not necessarily the ones that destroy property.*
    """)

    # --- Load Data ---
    df_human_cost_raw = load_data(sql_human_cost)

    st.divider()

    st.subheader("The Human Cost (Risk of Casualty)")
    with st.expander("📖 Read the Human Cost analysis", expanded=False):
        st.markdown("""
        Fires caused by **chip or fat pans** stand out as the most hazardous to people — roughly **1 in 3** of these incidents leads to an injury or fatality.
        They’re followed by **‘Playing with fire’** and **‘Careless handling’**.
        
        These findings point to a persistent theme:
        🔥 **Human behaviour and routine domestic activity drive the greatest casualty risk**, not mechanical faults or deliberate acts.
        """)
    
    # --- Filter data to Top 10 for a clean chart ---
    df_casualty_risk = df_human_cost_raw.sort_values(by="pct_chance_of_casualty", ascending=False).head(10)
    
    # --- Chart 3: Altair Vertical Column Chart (Labels Fixed & Added) ---
    base = alt.Chart(df_casualty_risk).encode(
        x=alt.X("cause_of_fire", 
                sort='-y', 
                title="Cause of Fire",
                axis=alt.Axis(labelAngle=-45)),
        y=alt.Y("pct_chance_of_casualty", 
                title="% of Incidents with Casualty"),
        color=alt.value("#ff6f3c"), # Apply a specific color
        tooltip=["cause_of_fire", "pct_chance_of_casualty", "number_of_incidents"]
    )
    
    bars = base.mark_bar()
    
    text = base.mark_text(
        align='center',
        baseline='bottom',
        dy=-5
    ).encode(
        text=alt.Text("pct_chance_of_casualty", format=".1f"),
        color=alt.value("black")
    )
    
    chart3 = (bars + text).properties(height=350).interactive()
    
    st.altair_chart(chart3, use_container_width=True)
    st.caption("Top 10 causes of fire, ranked by chance of casualty.")

    st.divider()

    st.subheader("The Property Cost (Destruction & Complexity)") 
    
    with st.expander("📖 Read the Property Cost analysis", expanded=False):
        st.markdown("""
        When shifting the lens to **property impact**, the pattern reverses.
        Fires from **unspecified causes** show the **widest average spread**, which strongly suggests the fire was so destructive, the cause was obliterated.
        
        Meanwhile, **‘Deliberate’** fires show the highest rate of **rescues**, marking them as the most complex *human* incidents for crews to resolve, distinct from the "high-interaction" casualty fires.
        """)
    
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Top 5 by Fire Spread (Destruction)**")
        
        # --- Filter data to Top 5 for a clean chart ---
        df_spread_risk = df_human_cost_raw.sort_values(by="avg_spread_rank", ascending=False).head(5)
        
        # --- Chart 4: Altair Vertical Column Chart (Labels Fixed & Added) ---
        base = alt.Chart(df_spread_risk).encode(
            x=alt.X("cause_of_fire", 
                    sort='-y', 
                    title="Cause of Fire",
                    axis=alt.Axis(labelAngle=-45)), # Angled labels
            y=alt.Y("avg_spread_rank", 
                    title="Average Fire Spread Rank"),
            color=alt.value("#ff9d5c"),
            tooltip=["cause_of_fire", "avg_spread_rank", "number_of_incidents"]
        )
        
        bars = base.mark_bar()
        
        text = base.mark_text(
            align='center',
            baseline='bottom',
            dy=-5
        ).encode(
            text=alt.Text("avg_spread_rank", format=".2f"),
            color=alt.value("black")
        )
        
        chart4 = (bars + text).properties(height=350).interactive()
        
        st.altair_chart(chart4, use_container_width=True)

    with col2:
        st.markdown("**Top 5 by Rescues (Complexity)**")
        
        # --- Filter data to Top 5 for a clean chart ---
        df_rescue_risk = df_human_cost_raw.sort_values(by="avg_rescues_per_incident", ascending=False).head(5)
        
        # --- Chart 5: Altair Vertical Column Chart (Labels Fixed & Added) ---
        base = alt.Chart(df_rescue_risk).encode(
            x=alt.X("cause_of_fire", 
                    sort='-y', 
                    title="Cause of Fire",
                    axis=alt.Axis(labelAngle=-45)), # Angled labels
            y=alt.Y("avg_rescues_per_incident", 
                    title="Avg. Rescues per Incident"),
            color=alt.value("#ffa600"),
            tooltip=["cause_of_fire", "avg_rescues_per_incident", "number_of_incidents"]
        )
        
        bars = base.mark_bar()
        
        text = base.mark_text(
            align='center',
        ).encode(
            text=alt.Text("avg_rescues_per_incident", format=".2f"),
            color=alt.value("black")
        )

        chart5 = (bars + text).properties(height=350).interactive()
        
        st.altair_chart(chart5, use_container_width=True)

# ---------------- PAGE 4: CONCLUSIONS ----------------
elif page == "Conclusions":
    st.title("💡 Conclusions & Strategic Insights")
    st.markdown("""
    This project successfully transformed raw, complex incident data into a queryable star schema, which in turn revealed several key strategic insights:

    ### 1. Response is driven by risk, not volume.
    - The analysis of vehicle response and fire locations confirms that the larger attendance for flats is a deliberate, risk-based policy.
    - It is not that fires in flats are more common, but that they have a unique and more dangerous risk profile due to communal spaces, which this analysis quantifies (e.g., **10%+** of flat fires occur in communal escape routes).

    ### 2. Human and property risk diverge.
    - The fires most likely to cause a **casualty** are not the fires most likely to cause **destruction**.
    - **Casualty risk** is highest with "high-interaction" domestic fires (e.g., **Chip/fat pans: 33% chance of casualty**).
    - **Property risk** is highest with highly destructive fires (e.g., **'Unspecified cause'** and **'Deliberate'**).

    ### 3. Operational complexity has two different drivers.
    - **Human Complexity:** `Deliberate` fires require the most **rescues**, making them the most complex incident from a life-saving intervention perspective.
    - **Property Complexity:** `Unspecified` fires have the highest **fire spread**, suggesting they are the most complex from a fire-suppression and property-damage perspective.

    ---

    ### 🔮 Implications
    These findings support a **two-track prevention strategy** for fire services:
    - **Behavioural Safety (Public Education):** A high-priority focus on **kitchen fire safety** (especially chip pans) and careless handling, as this is the primary driver of *human casualties*.
    - **Intervention & Engineering (Targeted Risk):** A specialist focus on **arson/firesetting prevention** to reduce high-complexity rescue incidents, and continued focus on **building safety and engineering** (e.g., refuse stores, corridors) in flats to mitigate their unique risk profile.
    """)

# ---------------- PAGE 5: METHODOLOGY ----------------
elif page == "Methodology & ETL":
    # --- **FIX**: Add JavaScript to scroll to top ---
    st.markdown("<script>window.scrollTo(0, 0);</script>", unsafe_allow_html=True)

    st.title("⚙️ Methodology & Project Details")
    st.markdown("This dashboard is the final output of an end-to-end data portfolio project, showcasing skills in ETL, data modelling, analysis, and BI visualisation.")
    st.markdown("---")

    st.subheader("1. Data Sourcing & ETL (Extract, Transform, Load)")
    st.markdown("""
    The data was sourced from the Home Office's Incident Recording System (IRS), provided as a series of `.ods` files.
    
    1.  **Extract:** A Python script using `BeautifulSoup` and `pandas` was built to scrape the GOV.UK statistics page for all incident-level dwelling fire datasets (2010-2025).
    2.  **Load (Staging):** The multiple `.ods` files were combined, cleaned, and loaded into a single `dwelling_fires_staging` table in a DuckDB database.
    3.  **Transform (Star Schema):** The core of the project was to transform this flat staging table into a performant **star schema**. This complex, SQL-based process involved:
        * Designing and creating **13 dimension tables** (e.g., `dim_ignition`, `dim_dwelling`, `dim_spread`).
        * Writing scripts to populate them with unique, distinct values from the staging table.
        * Handling significant data cleaning and transformation, especially for "bucketed" text columns (like `response_time`, `vehicles`, `personnel`). `CASE` statements and `regexp_matches` were used to convert text buckets (e.g., '4, 5 or 6') into queryable numeric midpoints (e.g., `5`).
        * Creating 0/1 flags (e.g., `fatality_casualty_flag`) from source text ('Fatality/Casualty', 'None').
        * Creating the final `fact_dwelling_fire` table by joining the staging table against all 13 new dimensions to look up their surrogate keys.
    """)
    st.markdown("---")
    
    st.subheader("2. Analysis & Visualisation")
    st.markdown("""
    The clean, efficient star schema I built made the complex analysis seen in this dashboard possible. 
    
    * **Backend:** SQL queries (like the one below) join the fact and dimension tables to aggregate data.
    * **Frontend:** `Streamlit`, `Pandas`, and `Altair` are used to run these queries, perform post-query transformations (like the 'House vs. Flats' percentage pivot), and render the interactive charts.
    * **Hosting:** The app is deployed on Streamlit Community Cloud.
    """)
    st.code(sql_to_display, language="sql")
    st.markdown("---")
    
    st.subheader("3. Cross-Validation")
    st.markdown("""
    Key findings were cross-validated against official Home Office statistical publications.
    
    * **Building Risk:** My analysis identified that 'Purpose Built Flats' have a unique risk profile from communal fires. This is strongly supported by an ad-hoc government report which found that fires in medium and high-rise flats are **more likely to result in a hospital casualty (6.9%)** than fires in houses (5.7%). The report also confirms that fire safety legislation (the FSO) specifically targets these **"common parts of residential buildings"**.
    
    * **Human vs. Property Risk:** My analysis provides a deeper, granular insight than the high-level summaries. It successfully identifies the *specific causes* driving casualty risk (e.g., 'Chip/ fat pan fires') and those driving destruction (e.g., 'Deliberate' and 'Unspecified cause'), which is a key finding for targeted fire prevention.
    """)
    st.markdown("---")

    st.subheader("Data Source and Licensing")
    st.markdown("""
    This analysis is based on public sector data sourced from the
    [Home Office Incident Recording System (IRS)](https://www.gov.uk/government/statistics/fire-statistics-incident-level-datasets)
    and is licensed under the
    [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
   .
    
    All data is © Crown copyright.
    """)