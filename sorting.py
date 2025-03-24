import json
import pandas as pd
from tabulate import tabulate
from statistics import mean
from typing import Dict, List, Any

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
    import matplotlib.pyplot as plt
    import matplotlib.dates
    from datetime import datetime
    
    try:
        import mplcursors
    except ImportError:
        print("Installing mplcursors for hover annotations...")
        import subprocess
        subprocess.check_call(["pip", "install", "mplcursors"])
        import mplcursors
    
    # Find all feeds matching the airport code exactly
    matching_feeds = {}
    for feed_name, feed_data in data.items():
        if 'static_data' in feed_data and 'icao' in feed_data['static_data']:
            if airport_code.upper() == feed_data['static_data']['icao'].upper():
                matching_feeds[feed_name] = feed_data
    
    if not matching_feeds:
        print(f"Identifier not found: {airport_code}")
        return
    
    print(f"Found {len(matching_feeds)} feeds for airport code: {airport_code}")
    
    # Create a figure with standard size
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Store line objects for later adding hover functionality
    lines = []
    
    for feed_name, feed_data in matching_feeds.items():
        if 'time_series' not in feed_data or not feed_data['time_series']:
            continue
            
        # Extract timestamps and listener counts
        timestamps = []
        listener_counts = []
        
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
                    listener_counts.append(entry['listeners'])
                except (ValueError, TypeError):
                    continue
        
        if timestamps and listener_counts:
            line, = ax.plot(timestamps, listener_counts, label=f"{feed_name}")
            lines.append((line, feed_name))
    
    ax.set_title(f"Listener Counts for {airport_code}")
    ax.set_xlabel("Time")
    ax.set_ylabel("Number of Listeners")
    ax.grid(True)
    plt.xticks(rotation=45)
    
    # Add hover annotations (enhanced to make up for no legend)
    cursor = mplcursors.cursor(hover=True)
    
    @cursor.connect("add")
    def on_add(sel):
        # Find which line was selected
        for line, feed_name in lines:
            if line == sel.artist:
                # Get the x, y data point that was hovered over
                x, y = sel.target
                timestamp = matplotlib.dates.num2date(x).strftime('%Y-%m-%d %H:%M:%S')
                sel.annotation.set_text(f"{feed_name}\nTime: {timestamp}\nListeners: {y:.0f}")
                break
    
    plt.tight_layout()
    
    # Remove PNG saving code, only show the plot
    print("Note: Hover over lines to see feed names and data points in the interactive window.")
    plt.show()

def sort_and_display(metrics: List[Dict[str, Any]], sort_by: str = 'avg_listeners', ascending: bool = False, limit: int = None, save_csv: bool = False):
    """Sort the metrics and display the results in a tabular format. Optionally save to CSV."""
    # Convert to DataFrame for easier sorting and display
    df = pd.DataFrame(metrics)
    
    # Sort the DataFrame
    df_sorted = df.sort_values(by=sort_by, ascending=ascending)
    
    # Apply limit if specified
    if limit:
        df_sorted = df_sorted.head(limit)
    
    if save_csv:
        # Create a filename based on the sort parameters
        filename = f"feeds_sorted_by_{sort_by}_{'asc' if ascending else 'desc'}"
        if limit:
            filename += f"_top{limit}"
        filename += ".csv"
        
        # Save to CSV
        df_sorted.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
    else:
        # Format the table for terminal display
        print(f"\nFeeds sorted by {sort_by} ({'ascending' if ascending else 'descending'}):")
        print(tabulate(df_sorted, headers='keys', tablefmt='grid', showindex=False, 
                      floatfmt='.2f'))
    
    return df_sorted

def main():
    file_path = "liveatc_feeds.json"  # Update this to your actual file path
    
    try:
        # Load data
        print(f"Loading data from {file_path}...")
        data = load_data(file_path)
        print(f"Loaded data for {len(data)} feeds.")
        
        # Calculate metrics
        metrics = calculate_metrics(data)
        print(f"Calculated metrics for {len(metrics)} feeds.")
        
        # Display options
        while True:
            print("\nSort options:")
            print("1. Average listeners (highest to lowest)")
            print("2. Maximum listeners (highest to lowest)")
            print("3. Average total listeners (highest to lowest)")
            print("4. ICAO code (alphabetical)")
            print("5. Location (alphabetical)")
            print("6. Custom sort")
            print("7. Export all data to CSV")
            print("8. Airport Histogram")
            print("0. Exit")
            
            choice = input("Enter your choice (0-8): ")
            
            if choice == '0':
                break
                
            if choice == '8':
                airport_code = input("Enter airport code (ICAO): ")
                start_date = input("Enter start date (YYYY-MM-DD) or leave blank: ")
                end_date = input("Enter end date (YYYY-MM-DD) or leave blank: ")
                display_airport_histogram(data, airport_code, start_date or None, end_date or None)
                continue
            
            if choice == '7':
                # Export all data without sorting
                df = pd.DataFrame(metrics)
                filename = "all_feeds_data.csv"
                df.to_csv(filename, index=False)
                print(f"All data saved to {filename}")
                continue
                
            limit_input = input("Limit results? Enter number or leave blank for all: ")
            limit = int(limit_input) if limit_input.strip() else None
            
            # Ask if user wants to save to CSV
            save_csv = input("Save to CSV? (y/n): ").lower() == 'y'
            
            if choice == '1':
                sort_and_display(metrics, 'avg_listeners', False, limit, save_csv)
            elif choice == '2':
                sort_and_display(metrics, 'max_listeners', False, limit, save_csv)
            elif choice == '3':
                sort_and_display(metrics, 'avg_total_listeners', False, limit, save_csv)
            elif choice == '4':
                sort_and_display(metrics, 'icao', True, limit, save_csv)
            elif choice == '5':
                sort_and_display(metrics, 'location', True, limit, save_csv)
            elif choice == '6':
                columns = metrics[0].keys() if metrics else []
                print(f"Available columns: {', '.join(columns)}")
                sort_column = input("Enter column name to sort by: ")
                if sort_column in columns:
                    ascending_input = input("Sort ascending? (y/n): ").lower()
                    ascending = ascending_input == 'y'
                    sort_and_display(metrics, sort_column, ascending, limit, save_csv)
                else:
                    print(f"Invalid column name: {sort_column}")
            else:
                print("Invalid choice, please try again.")
                
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {file_path}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()