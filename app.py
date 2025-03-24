import json
import pandas as pd
from statistics import mean
from typing import Dict, List, Any
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.dates
from datetime import datetime, timezone
import plotly.graph_objects as go
import os

def load_data(file_path: str) -> Dict[str, Any]:
    """Load JSON data from the specified file path."""
    with open(file_path, 'r') as file:
        return json.load(file)

def calculate_metrics(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Calculate listener count metrics for each feed."""
    results = []
    
    for feed_name, feed_data in data.items():
        if 'time_series' not in feed_data:
            continue
            
        time_series = feed_data['time_series']
        
        # Skip feeds with no time series data
        if not time_series:
            continue
            
        listener_counts = [entry.get('listeners', 0) for entry in time_series]
        total_listener_counts = [entry.get('total_listeners', 0) for entry in time_series]
        
        # Calculate metrics
        avg_listeners = mean(listener_counts) if listener_counts else 0
        max_listeners = max(listener_counts) if listener_counts else 0
        avg_total_listeners = mean(total_listener_counts) if total_listener_counts else 0
        
        # Get location and ICAO if available
        static_data = feed_data.get('static_data', {})
        location = static_data.get('location', 'N/A')
        icao = static_data.get('icao', 'N/A')
        channel_types = ', '.join(static_data.get('channel_types', ['N/A']))
        
        results.append({
            'feed_name': feed_name,
            'icao': icao,
            'location': location,
            'channel_types': channel_types,
            'avg_listeners': avg_listeners,
            'max_listeners': max_listeners,
            'avg_total_listeners': avg_total_listeners,
            'data_points': len(time_series)
        })
    
    return results

def display_airport_histogram(data: Dict[str, Any], airport_code: str, start_date: str = None, end_date: str = None):
    """
    Display a histogram of listener counts for a specific airport over time.
    
    Args:
        data: The loaded feed data
        airport_code: ICAO or other identifier to search for
        start_date: Optional start date in format YYYY-MM-DD
        end_date: Optional end date in format YYYY-MM-DD
    """
    # Find all feeds matching the airport code exactly
    matching_feeds = {}
    for feed_name, feed_data in data.items():
        if 'static_data' in feed_data and 'icao' in feed_data['static_data']:
            if airport_code.upper() == feed_data['static_data']['icao'].upper():
                matching_feeds[feed_name] = feed_data
    
    if not matching_feeds:
        st.error(f"Identifier not found: {airport_code}")
        return
    
    st.write(f"Found {len(matching_feeds)} feeds for airport code: {airport_code}")
    
    # Create a Plotly figure for interactive visualization
    fig = go.Figure()
    
    # Process each feed
    for feed_name, feed_data in matching_feeds.items():
        if 'time_series' not in feed_data or not feed_data['time_series']:
            continue
            
        # Extract timestamps and listener counts
        timestamps = []
        listener_counts = []
        hover_texts = []
        
        for entry in feed_data['time_series']:
            if 'timestamp' in entry and 'listeners' in entry:
                try:
                    # Convert timestamp to datetime
                    dt = datetime.fromisoformat(entry['timestamp'].replace('Z', '+00:00'))
                    
                    # Apply date filtering if specified
                    if start_date:
                        start_dt = datetime.fromisoformat(f"{start_date}T00:00:00+00:00")
                        if dt < start_dt:
                            continue
                    if end_date:
                        end_dt = datetime.fromisoformat(f"{end_date}T23:59:59+00:00") 
                        if dt > end_dt:
                            continue
                            
                    timestamps.append(dt)
                    listeners = entry['listeners']
                    listener_counts.append(listeners)
                    
                    # Create hover text with details
                    hover_text = f"Feed: {feed_name}<br>" + \
                                 f"Time: {dt.strftime('%Y-%m-%d %H:%M:%S')}<br>" + \
                                 f"Listeners: {listeners}"
                    hover_texts.append(hover_text)
                    
                except (ValueError, TypeError):
                    continue
        
        if timestamps and listener_counts:
            # Add a trace for this feed
            fig.add_trace(go.Scatter(
                x=timestamps,
                y=listener_counts,
                mode='lines',
                name=feed_name,
                hoverinfo='text',
                hovertext=hover_texts,
                line=dict(width=2),
            ))
    
    # Customize layout
    fig.update_layout(
        title=f"Listener Counts for {airport_code}",
        xaxis_title="Time",
        yaxis_title="Number of Listeners",
        legend_title="Feeds",
        hovermode="closest",
        height=600,
        width=None,  # Let Streamlit determine the width
        margin=dict(l=20, r=20, t=40, b=20),
    )
    
    # Add grid
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightGrey')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='LightGrey')
    
    # Display the interactive plot in Streamlit
    st.plotly_chart(fig, use_container_width=True)

def sort_and_display(metrics: List[Dict[str, Any]], sort_by: str = 'avg_listeners', ascending: bool = False, limit: int = None):
    """Sort the metrics and display the results in a tabular format."""
    # Convert to DataFrame for easier sorting and display
    df = pd.DataFrame(metrics)
    
    # Sort the DataFrame
    df_sorted = df.sort_values(by=sort_by, ascending=ascending)
    
    # Apply limit if specified
    if limit:
        df_sorted = df_sorted.head(limit)
    
    # Add a row number column that starts at 1
    df_sorted = df_sorted.reset_index(drop=True)
    df_sorted.index = df_sorted.index + 1  # Start indexing at 1 instead of 0
    
    # Rename the index column to "Rank"
    df_sorted_with_rank = df_sorted.rename_axis("Rank")
    
    # Display using Streamlit with the index (now showing rank numbers)
    st.dataframe(df_sorted_with_rank)
    
    # For CSV export, we need to reset the index to make it a regular column
    df_for_export = df_sorted.reset_index().rename(columns={"index": "Rank"})
    
    return df_for_export

def main():
    st.set_page_config(page_title="LiveATC Feed Analyzer", layout="wide")
    
    st.title("LiveATC Feed Analyzer")
    st.write("Analyze listener data from LiveATC feeds")
    
    # Get last modified date for local file if it exists
    local_file_path = "liveatc_feeds.json"
    local_file_label = "Use local file (liveatc_feeds.json)"
    
    try:
        # Get file modification time
        mod_time = os.path.getmtime(local_file_path)
        # Convert to datetime and format for display
        mod_date = datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d %H:%M:%S')
        local_file_label = f"Use local file (liveatc_feeds.json, data ending on: {mod_date})"
    except (FileNotFoundError, OSError):
        # If file doesn't exist or other OS error, just use the default label
        pass
    
    # Option to choose data source with updated label
    data_source = st.radio(
        "Choose data source:",
        [local_file_label, "Upload a JSON file"]
    )
    
    data = None
    
    if data_source == local_file_label:  # Use updated label in condition
        # Use the local file
        try:
            with st.spinner('Loading local file...'):
                data = load_data(local_file_path)
            st.success(f"Loaded local data for {len(data)} feeds.")
        except FileNotFoundError:
            st.error(f"Local file not found at {local_file_path}. Please upload a file instead.")
            data_source = "Upload a JSON file"  # Switch to upload option
        except json.JSONDecodeError:
            st.error(f"Invalid JSON format in the local file. Please upload a valid file instead.")
            data_source = "Upload a JSON file"  # Switch to upload option
    
    if data_source == "Upload a JSON file":
        # File uploader
        uploaded_file = st.file_uploader("Upload LiveATC feeds JSON file", type="json")
        
        if uploaded_file is not None:
            # Load data from uploaded file
            with st.spinner('Loading uploaded file...'):
                data = json.load(uploaded_file)
            st.success(f"Loaded uploaded data for {len(data)} feeds.")
    
    # Continue only if we have data
    if data is not None:
        # Calculate metrics
        with st.spinner('Calculating metrics...'):
            metrics = calculate_metrics(data)
        st.write(f"Calculated metrics for {len(metrics)} feeds.")
        
        # Sidebar for analysis options
        st.sidebar.header("Analysis Options")
        analysis_option = st.sidebar.radio(
            "Choose Analysis Type",
            ["Sort & Filter Feeds", "Airport Histogram", "Export Data"]
        )
        
        if analysis_option == "Sort & Filter Feeds":
            st.header("Sort & Filter Feeds")
            
            # Create a 2-column layout for sort selector and limit input
            col1, col2 = st.columns(2)
            
            # Sort options in the first column
            with col1:
                sort_options = {
                    "Average listeners": "avg_listeners",
                    "Maximum listeners": "max_listeners",
                    "Average total listeners": "avg_total_listeners",
                    "ICAO code": "icao",
                    "Location": "location"
                }
                
                sort_by = st.selectbox(
                    "Sort by",
                    options=list(sort_options.keys())
                )
                
                # Convert display name to column name
                sort_column = sort_options[sort_by]
            
            # Limit input in the second column
            with col2:
                limit = st.number_input("Limit results (0 for all)", min_value=0, value=0)
                limit = None if limit == 0 else int(limit)
            
            # Ascending checkbox below both columns
            ascending = st.checkbox("Sort ascending", value=False)
            
            # Display the sorted data
            sorted_data = sort_and_display(metrics, sort_column, ascending, limit)
            
            # Add download button
            if st.button("Download as CSV"):
                csv = sorted_data.to_csv(index=False)
                st.download_button(
                    label="Download CSV file",
                    data=csv,
                    file_name=f"feeds_sorted_by_{sort_column}.csv",
                    mime="text/csv"
                )
                
        elif analysis_option == "Airport Histogram":
            st.header("Airport Histogram")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                airport_code = st.text_input("Enter airport code (ICAO)").strip()
            
            with col2:
                start_date = st.date_input("Start date (optional)", value=None)
                start_date_str = start_date.strftime("%Y-%m-%d") if start_date else None
            
            with col3:
                end_date = st.date_input("End date (optional)", value=None)
                end_date_str = end_date.strftime("%Y-%m-%d") if end_date else None
            
            if airport_code:
                if st.button("Generate Histogram"):
                    display_airport_histogram(data, airport_code, start_date_str, end_date_str)
            else:
                st.info("Please enter an airport code to generate a histogram.")
                
        elif analysis_option == "Export Data":
            st.header("Export All Data")
            
            # Create DataFrame for all data
            df = pd.DataFrame(metrics)
            
            st.dataframe(df)
            
            # Add download button for full dataset
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download Complete Dataset as CSV",
                data=csv,
                file_name="liveatc_all_feeds_data.csv",
                mime="text/csv"
            )
    else:
        st.info("Please select a data source to begin analysis.")
        st.markdown("""
        ### Sample JSON Format:
        ```json
        {
            "feed_name": {
                "static_data": {
                    "location": "Airport Name",
                    "icao": "KABC",
                    "channel_types": ["Ground", "Tower"]
                },
                "time_series": [
                    {
                        "timestamp": "2023-01-01T00:00:00Z",
                        "listeners": 42,
                        "total_listeners": 100
                    },
                    ...
                ]
            },
            ...
        }
        ```
        """)

if __name__ == "__main__":
    main()