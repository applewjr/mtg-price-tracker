# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
import pandas as pd
import hashlib

# Initialize session state for selected card ID
if 'selected_card_id' not in st.session_state:
    st.session_state.selected_card_id = "ecc1027a-8c07-44a0-bdde-fa2844cff694"

# Create fresh Snowflake session on-demand (no caching!)
def get_snowflake_session():
    """Create fresh Snowflake session on-demand"""
    try:
        session = get_active_session()
        return session, "Connected using active Snowflake session"
    except:
        try:
            connection_parameters = {
                "account": st.secrets["snowflake"]["account"],
                "user": st.secrets["snowflake"]["user"],
                "password": st.secrets["snowflake"]["password"],
                "warehouse": st.secrets["snowflake"]["warehouse"],
                "database": st.secrets["snowflake"]["database"],
                "schema": st.secrets["snowflake"]["schema"]
            }
            session = Session.builder.configs(connection_parameters).create()
            return session, "Connected to Snowflake using credentials"
        except Exception as e:
            st.error(f"Failed to connect to Snowflake: {e}")
            st.stop()

# Cache card search results for 24 hours (card database is relatively static)
@st.cache_data(ttl=86400)  # Cache for 24 hours
def search_cards(search_term1, search_term2):
    """Search for cards and cache results"""
    session, _ = get_snowflake_session()
    try:
        search_query = f"SELECT * FROM TABLE(MTG_COST.PUBLIC.GET_CARD_ID('{search_term1}', '{search_term2}')) LIMIT 1000"
        search_result = session.sql(search_query)
        search_df = search_result.to_pandas()
        return search_df
    except Exception as e:
        st.error(f"Error searching cards: {str(e)}")
        return pd.DataFrame()

# Cache price data for 24 hours (prices only update once per day)
@st.cache_data(ttl=86400)  # Cache for 24 hours
def get_card_prices(card_id):
    """Get card price data and cache results"""
    session, _ = get_snowflake_session()
    try:
        query = f"SELECT * FROM TABLE(MTG_COST.PUBLIC.GET_CARD_PRICES('{card_id}'))"
        result = session.sql(query)
        df = result.to_pandas()
        return df
    except Exception as e:
        st.error(f"Error querying data: {str(e)}")
        return pd.DataFrame()

# Cache view data for 24 hours
@st.cache_data(ttl=86400)  # Cache for 24 hours
def get_price_after_launch():
    """Get price after launch data and cache results"""
    session, _ = get_snowflake_session()
    try:
        query = "SELECT * FROM price_after_launch"
        result = session.sql(query)
        df = result.to_pandas()
        return df
    except Exception as e:
        st.error(f"Error querying price after launch data: {str(e)}")
        return pd.DataFrame()

# Initialize session for display message
session, connection_message = get_snowflake_session()
st.success(connection_message)
        
# Write directly to the app
st.title("MTG Card Price Tracker 🃏")
st.write(
    """Track Magic: The Gathering card prices over time.
    This dashboard shows price trends for both regular and foil versions of cards.
    """
)

# Cache status indicator
with st.expander("ℹ️ Cache Information"):
    st.write("""
    **Caching Strategy:**
    - Card searches: Cached for 24 hours
    - Price data: Cached for 24 hours (updates once daily)
    - Sessions: Fresh connections on-demand
    
    This minimizes Snowflake compute costs while ensuring reliability.
    """)

# Card Search Section
st.subheader("🔍 Find Card ID")
st.write("Search for cards to get their UUID for price tracking")

col1, col2 = st.columns(2)
with col1:
    search_term1_raw = st.text_input(
        "Card Name", 
        value="vivi",
        help="Enter part of the card name"
    )
    search_term1 = search_term1_raw.lower()

with col2:
    search_term2_raw = st.text_input(
        "Set Name", 
        value="final fantasy",
        help="Enter additional search criteria"
    )
    search_term2 = search_term2_raw.lower()

# Add cache control
col1, col2 = st.columns([3, 1])
with col1:
    search_button = st.button("Search Cards")
with col2:
    if st.button("🔄 Clear Cache"):
        st.cache_data.clear()
        st.success("Cache cleared!")
        st.rerun()

if search_button or (search_term1 and search_term2):
    # Show cache status
    cache_key = f"{search_term1}_{search_term2}"
    
    with st.spinner("Searching for cards..."):
        search_df = search_cards(search_term1, search_term2)
        
    if not search_df.empty:
        st.write(f"Found {len(search_df)} cards:")
        
        # Reorder columns for better display
        column_order = [
            'NAME', 'TCGPLAYER_URL', 'SET_NAME', 'ID', 
            'AVG_PRICE', 'MIN_PRICE', 'MAX_PRICE', 
            'AVG_FOIL_PRICE', 'MIN_FOIL_PRICE', 'MAX_FOIL_PRICE', 
            'PRICE_RECORDS_COUNT'
        ]
        
        # Reorder the dataframe columns
        search_df_ordered = search_df[column_order]
        
        # Configure column display with clickable URLs
        column_config = {
            "TCGPLAYER_URL": st.column_config.LinkColumn(
                "TCGPlayer Link",
                help="Click to open card page on TCGPlayer",
                display_text="View on TCGPlayer"
            )
        }
        
        # Make the dataframe interactive with clickable URLs and selection
        event = st.dataframe(
            search_df_ordered, 
            use_container_width=True,
            column_config=column_config,
            on_select="rerun",
            selection_mode="single-row"
        )
        
        # Handle row selection to populate card ID
        if len(event.selection.rows) > 0:
            selected_row = event.selection.rows[0]
            selected_card_id = search_df_ordered.iloc[selected_row]['ID']
            if st.session_state.selected_card_id != selected_card_id:
                st.session_state.selected_card_id = selected_card_id
                st.success(f"✅ Selected card ID: {selected_card_id}")
                st.rerun()
        
        # Show instruction for selecting cards
        st.info("💡 Click on any row to select that card for price tracking below")
    else:
        st.warning("No cards found matching your search terms.")

st.divider()  # Visual separator

# Price Tracking Section
st.subheader("📈 Card Price Tracker")

# Card ID input - now uses session state for selected card
card_id = st.text_input(
    "Card ID", 
    value=st.session_state.selected_card_id,
    key="card_id_input",
    help="Enter the card UUID to track price history (or select from search above)"
)

# Update session state if user manually types in the input
if card_id != st.session_state.selected_card_id:
    st.session_state.selected_card_id = card_id

if card_id:
    with st.spinner("Loading price data..."):
        df = get_card_prices(card_id)
    
    if not df.empty:
        # Convert PULL_DATE to datetime if it's not already
        df['PULL_DATE'] = pd.to_datetime(df['PULL_DATE'])
        
        # Sort by date for proper line chart
        df = df.sort_values('PULL_DATE')
        
        # Display current prices
        latest_data = df.iloc[-1]
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Latest Regular Price", 
                f"${latest_data['USD']:.2f}" if pd.notna(latest_data['USD']) else "N/A"
            )
        
        with col2:
            st.metric(
                "Latest Foil Price", 
                f"${latest_data['USD_FOIL']:.2f}" if pd.notna(latest_data['USD_FOIL']) else "N/A"
            )
        
        with col3:
            st.metric(
                "Last Updated", 
                latest_data['PULL_DATE'].strftime('%Y-%m-%d')
            )
        
        # Price trend chart
        st.subheader("Price Trends Over Time")
        
        # Prepare data for chart
        chart_data = df.set_index('PULL_DATE')[['USD', 'USD_FOIL']].rename(columns={
            'USD': 'Regular Price',
            'USD_FOIL': 'Foil Price'
        })
        
        # Remove rows where both prices are null
        chart_data = chart_data.dropna(how='all')
        
        if not chart_data.empty:
            st.line_chart(chart_data)
        else:
            st.warning("No price data available for charting.")
        
        # Raw data table
        with st.expander("📊 Price History Data"):
            # Format the dataframe for display
            display_df = df.copy()
            display_df['PULL_DATE'] = display_df['PULL_DATE'].dt.strftime('%Y-%m-%d')
            display_df['USD'] = display_df['USD'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "N/A")
            display_df['USD_FOIL'] = display_df['USD_FOIL'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "N/A")
            
            st.dataframe(display_df, use_container_width=True)
        
        # Summary statistics
        st.subheader("Price Statistics")
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Regular Card Statistics:**")
            regular_prices = df['USD'].dropna()
            if not regular_prices.empty:
                st.write(f"- Average: ${regular_prices.mean():.2f}")
                st.write(f"- Minimum: ${regular_prices.min():.2f}")
                st.write(f"- Maximum: ${regular_prices.max():.2f}")
                st.write(f"- Data Points: {len(regular_prices)}")
            else:
                st.write("No regular price data available")
        
        with col2:
            st.write("**Foil Card Statistics:**")
            foil_prices = df['USD_FOIL'].dropna()
            if not foil_prices.empty:
                st.write(f"- Average: ${foil_prices.mean():.2f}")
                st.write(f"- Minimum: ${foil_prices.min():.2f}")
                st.write(f"- Maximum: ${foil_prices.max():.2f}")
                st.write(f"- Data Points: {len(foil_prices)}")
            else:
                st.write("No foil price data available")
    
    else:
        st.warning("No data found for this card ID.")

else:
    st.info("Please enter a card ID to view price data.")

st.divider()  # Visual separator

# Price After Launch Analysis Section
st.subheader("📊 Average Price of Mythic and Rare Cards Per Set")
st.write("Track how card prices evolve over the first 300 days after set release")

with st.spinner("Loading price analysis data..."):
    launch_df = get_price_after_launch()

if not launch_df.empty:
    # Prepare data for chart (pivot to get sets as separate series)
    chart_data = launch_df.pivot(index='DATE_DIFF', columns='SET_NAME', values='AVG_USD')
    
    # Sort by date_diff for proper line chart
    chart_data = chart_data.sort_index()
    
    # Create the line chart
    st.line_chart(chart_data, x_label="Days After Launch", y_label="Average USD")
    
    # Show summary statistics
    with st.expander("📈 Price Analysis Summary"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Sets Tracked:**")
            sets = launch_df['SET_NAME'].unique()
            for set_name in sorted(sets):
                set_data = launch_df[launch_df['SET_NAME'] == set_name]
                avg_price = set_data['AVG_USD'].mean()
                st.write(f"- {set_name}: ${avg_price:.2f} avg")
        
        with col2:
            st.write("**Price Trends:**")
            st.write(f"- Total data points: {len(launch_df):,}")
            st.write(f"- Date range: 1-300 days after release")
            st.write(f"- Card types: Mythic & Rare only")
            st.write(f"- Sets: {len(sets)} expansion sets")

else:
    st.warning("No price after launch data available.")

# Footer with cache info
st.markdown("---")
st.caption("💡 Data cached for 24 hours to minimize costs. Prices update once daily in the database.")