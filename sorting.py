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
    file_path = "liveatc_feeds1.json"  # Update this to your actual file path
    
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
            print("0. Exit")
            
            choice = input("Enter your choice (0-7): ")
            
            if choice == '0':
                break
            
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