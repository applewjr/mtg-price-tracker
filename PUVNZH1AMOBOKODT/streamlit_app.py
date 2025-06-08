# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

# Write directly to the app
st.title("MTG Card Price Tracker 🃏")
st.write(
    """Track Magic: The Gathering card prices over time.
    This dashboard shows price trends for both regular and foil versions of cards.
    """
)

# Get the current credentials
session = get_active_session()

# Card Search Section
st.subheader("🔍 Find Card ID")
st.write("Search for cards to get their UUID for price tracking")

col1, col2 = st.columns(2)
with col1:
    search_term1 = st.text_input(
        "First search term", 
        value="vaan",
        help="Enter part of the card name"
    )

with col2:
    search_term2 = st.text_input(
        "Second search term", 
        value="final",
        help="Enter additional search criteria"
    )

if st.button("Search Cards") or (search_term1 and search_term2):
    try:
        # Execute the card search query
        search_query = f"SELECT * FROM TABLE(MTG_COST.PUBLIC.GET_CARD_ID('{search_term1}', '{search_term2}')) LIMIT 100"
        search_result = session.sql(search_query)
        search_df = search_result.to_pandas()
        
        if not search_df.empty:
            st.write(f"Found {len(search_df)} cards:")
            
            # Make the dataframe interactive - user can click to copy card ID
            st.dataframe(search_df, use_container_width=True)
            
            # Show instruction for copying card ID
            st.info("💡 Copy a card ID from the table above to use in the price tracker below")
        else:
            st.warning("No cards found matching your search terms.")
            
    except Exception as e:
        st.error(f"Error searching cards: {str(e)}")

st.divider()  # Visual separator

# Price Tracking Section
st.subheader("📈 Card Price Tracker")

# Card ID input (you can make this dynamic later)
card_id = st.text_input(
    "Card ID", 
    value="883c6111-c921-4cd6-930d-4fa335ef2871",
    help="Enter the card UUID to track price history (use search above to find card IDs)"
)

if card_id:
    try:
        # Execute the query using the card ID
        query = f"SELECT * FROM TABLE(MTG_COST.PUBLIC.GET_CARD_PRICES('{card_id}'))"
        result = session.sql(query)
        
        # Convert to pandas dataframe
        df = result.to_pandas()
        
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
            st.subheader("Price History Data")
            
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
                else:
                    st.write("No regular price data available")
            
            with col2:
                st.write("**Foil Card Statistics:**")
                foil_prices = df['USD_FOIL'].dropna()
                if not foil_prices.empty:
                    st.write(f"- Average: ${foil_prices.mean():.2f}")
                    st.write(f"- Minimum: ${foil_prices.min():.2f}")
                    st.write(f"- Maximum: ${foil_prices.max():.2f}")
                else:
                    st.write("No foil price data available")
        
        else:
            st.warning("No data found for this card ID.")
            
    except Exception as e:
        st.error(f"Error querying data: {str(e)}")
        st.write("Please check that the card ID is valid and the function exists.")

else:
    st.info("Please enter a card ID to view price data.")