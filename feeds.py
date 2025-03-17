import asyncio
import aiohttp
from bs4 import BeautifulSoup
import json
import re
import os
from datetime import datetime, timezone
import time

# Data structure to hold our extracted information
class AirportFeed:
    def __init__(self, icao, location, status, listeners, total_listeners, 
                 channel_types, metar, frequencies, timestamp, feed_name):
        self.icao = icao
        self.location = location
        self.status = status
        self.listeners = listeners
        self.total_listeners = total_listeners
        self.channel_types = channel_types
        self.metar = metar
        self.frequencies = frequencies
        self.timestamp = timestamp
        self.feed_name = feed_name
    
    def to_dict(self):
        return {
            "icao": self.icao,
            "location": self.location,
            "status": self.status,
            "listeners": self.listeners,
            "total_listeners": self.total_listeners,
            "channel_types": self.channel_types,
            "metar": self.metar,
            "frequencies": self.frequencies,
            "timestamp": self.timestamp,
            "feed_name": self.feed_name
        }

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

def extract_feed_data(html):
    soup = BeautifulSoup(html, 'html.parser')
    feeds = []
    
    # Find the main table containing feed information
    main_table = None
    for table in soup.find_all('table'):
        if (table.get('width') == '900' and 
            table.get('border') == '1' and 
            table.get('bordercolor') == '#333333' and 
            table.get('bgcolor') == '#EEEEEE'):
            main_table = table
            break
    
    if not main_table:
        print("Could not find the main feed table")
        return feeds
    
    # Find all rows that contain cells with the target background colors
    feed_cells = main_table.select('td[bgcolor="lightgreen"], td[bgcolor="pink"]')
    feed_rows = [cell.find_parent('tr') for cell in feed_cells]
    
    for row in feed_rows:
        try:
            # Extract ICAO code from the name attribute of the anchor tag
            icao_element = row.find('a', {'name': True})
            if not icao_element:
                continue
                
            icao = icao_element.get('name')
            
            # Extract location - it's in a font tag with class="nav"
            location = "Unknown"
            nav_fonts = row.find_all('font', {'class': 'nav'})
            if nav_fonts and len(nav_fonts) > 0:
                # The location is typically in the first font tag with class="nav"
                location = nav_fonts[0].text.strip()
            
            # Extract status (UP/DOWN) - it's in a font tag within a span with class="purSep"
            status = "Unknown"
            status_span = row.find('span', {'class': 'purSep'})
            if status_span:
                status_font = status_span.find('font')
                if status_font:
                    status = status_font.text.strip()
            
            # Extract channel types from the title in the strong tag
            # use the frequencies for this
            channel_types = []
            # print(row)
            strong_tag = row.find('strong')
            if strong_tag:
                title_text = strong_tag.text
                if 'Twr' in title_text:
                    channel_types.append('Tower')
                if 'App' in title_text:
                    channel_types.append('Approach')
                if 'Dep' in title_text:
                    channel_types.append('Departure')
                if 'Gnd' in title_text:
                    channel_types.append('Ground')
                if 'Ctr' in title_text or 'Center' in title_text:
                    channel_types.append('Center')
                if 'ATIS' in title_text:
                    channel_types.append('ATIS')
            
            # Extract METAR if available - it's after a <br /> tag within the purSep span
            metar_text = ""
            if status_span:
                # Convert the status_span to string to search for METAR
                status_span_str = str(status_span)
                
                # Find METAR pattern - typically starts with 4-letter ICAO code followed by date/time and weather info
                # Example: FIMP 170800Z 04011KT 9999 FEW015CB SCT017 31/25 Q1011
                metar_pattern = re.compile(r'<br />((?:[A-Z]{4}\s\d{6}Z.+?)(?=<br />|$))')
                metar_match = metar_pattern.search(status_span_str)
                
                if metar_match:
                    metar_text = metar_match.group(1).strip()
                else:
                    # Alternative pattern for some feeds
                    alt_pattern = re.compile(r'UTC</font><br><br />((?:[A-Z]{4}\s\d{6}Z.+?)(?=<br />|$))')
                    alt_match = alt_pattern.search(status_span_str)
                    if alt_match:
                        metar_text = alt_match.group(1).strip()
                
                # If still no match, try a more general approach
                if not metar_text:
                    # Look for any text that looks like a METAR (ICAO code followed by date/time)
                    general_pattern = re.compile(r'([A-Z]{4}\s\d{6}Z\s+[\w\d\s/]+\s+Q\d{4}(?:\s+\w+)?)')
                    general_match = general_pattern.search(status_span_str)
                    if general_match:
                        metar_text = general_match.group(1).strip()
                
                # If still no match, try an even more general pattern
                if not metar_text:
                    # Look for any text that starts with an ICAO code and date/time
                    basic_pattern = re.compile(r'([A-Z]{4}\s\d{6}Z\s+[^<]+)')
                    basic_match = basic_pattern.search(status_span_str)
                    if basic_match:
                        metar_text = basic_match.group(1).strip()
                
                # Clean up the METAR text - remove any HTML tags that might have been captured
                if metar_text:
                    # Remove any HTML tags
                    metar_text = re.sub(r'<[^>]+>', '', metar_text)
                    # Remove any extra whitespace
                    metar_text = re.sub(r'\s+', ' ', metar_text).strip()
                    
                    # Validate the METAR format - it should start with an ICAO code (4 uppercase letters)
                    if not re.match(r'^[A-Z]{4}\s\d{6}Z', metar_text):
                        metar_text = ""  # Invalid METAR format, reset to empty
            
            # Extract listener count - it's in a font tag with class="purSep" after the status span
            listeners = 0
            total_listeners = 0
            listener_fonts = row.find_all('font', {'class': 'purSep'})
            for font in listener_fonts:
                if 'Listeners:' in font.text:
                    listeners_pattern = re.compile(r'Listeners:\s*(\d+)\s*out of\s*(\d+)')
                    listeners_match = listeners_pattern.search(font.text)
                    if listeners_match:
                        listeners = int(listeners_match.group(1))
                        total_listeners = int(listeners_match.group(2))
                        break
            
            # Extract frequencies - they're in the next cell (td) with valign="top"
            frequencies = ""
            # Get all td elements in the current row
            tds = row.find_all('td')
            if len(tds) > 1:
                # Try to find the td with frequencies - it will be the last td with valign="top"
                freq_td = None
                for td in tds:
                    if td.get('valign') == 'top' and td.select_one('p font.purSep'):
                        freq_td = td
                        break
                
                if freq_td:
                    freq_font = freq_td.select_one('p font.purSep')
                    if freq_font:
                        # Split the text by newlines and remove empty lines
                        freq_text = freq_font.text.strip()
                        freq_lines = [line.strip() for line in freq_text.split('\n') if line.strip()]
                        
                        # Process each frequency line

                        frequencies = freq_lines[0]
            
            # print("\nExtracted frequencies:", frequencies)
            
            # Extract additional channel types from frequencies string
            if frequencies:
                # Define mappings for channel type detection
                channel_type_mappings = {
                    'Tower': ['Tower', 'Twr', 'TWR'],
                    'Approach': ['Approach', 'App', 'APP', 'Arrival', 'ARR'],
                    'Departure': ['Departure', 'Dep', 'DEP'],
                    'Ground': ['Ground', 'Gnd', 'GND'],
                    'Center': ['Center', 'Centre', 'Ctr', 'CTR', 'Control', 'CTRL'],
                    'ATIS': ['ATIS', 'Information', 'Info', 'INFO'],
                    'Clearance': ['Clearance', 'Clnc', 'CLNC', 'Delivery', 'Del', 'DEL'],
                    'Ramp': ['Ramp', 'Apron', 'APN'],
                    'Operations': ['Operations', 'Ops', 'OPS'],
                    'Radio': ['Radio', 'Unicom', 'UNICOM'],
                    'Director': ['Director', 'Dir', 'DIR'],
                    'Radar': ['Radar', 'RAD'],
                    'Terminal': ['Terminal', 'TMA'],
                    'Area': ['Area', 'ACC'],
                    'Flight Service': ['Flight Service', 'FSS'],
                    'Surface': ['Surface', 'SMC'],
                    'Pre-Departure': ['Pre-Departure', 'PDC'],
                    'Final': ['Final', 'FIN'],
                    'Emergency': ['Emergency', 'EMERG']
                }
                
                # Convert frequencies to lowercase for case-insensitive matching
                frequencies_lower = frequencies.lower()
                
                # Check for each channel type in the frequencies string
                for channel_type, keywords in channel_type_mappings.items():
                    for keyword in keywords:
                        # Check for exact word match (surrounded by spaces, colon, or at beginning/end)
                        keyword_lower = keyword.lower()
                        if (f" {keyword_lower} " in f" {frequencies_lower} " or 
                            f" {keyword_lower}: " in frequencies_lower or 
                            f":{keyword_lower} " in frequencies_lower or
                            frequencies_lower.startswith(f"{keyword_lower} ") or
                            frequencies_lower.endswith(f" {keyword_lower}") or
                            f" {keyword_lower}/" in frequencies_lower or
                            f"/{keyword_lower} " in frequencies_lower):
                            if channel_type not in channel_types:
                                channel_types.append(channel_type)
                                break
            
            # Current timestamp in UTC
            timestamp = datetime.now(timezone.utc).isoformat()
            
            # Extract feed name from the strong tag text
            feed_name = "Unknown Feed"
            if strong_tag and strong_tag.find('a'):
                feed_name = strong_tag.find('a').text.strip()
            elif strong_tag and strong_tag.text.strip() != "DOWN":
                feed_name = strong_tag.text.strip()
            else:
                # For DOWN feeds or when strong tag is not available
                # The feed name is in the first font tag with class="nav"
                nav_fonts = row.find_all('font', {'class': 'nav'})
                if nav_fonts and len(nav_fonts) > 0:
                    # Get the text content of the first nav font
                    nav_text = nav_fonts[0].get_text(separator=' ', strip=True)
                    
                    # For DOWN feeds, we need to construct the feed name from the ICAO code and the channel types
                    # The nav text typically contains the ICAO code followed by the channel types (e.g., "FLKK Twr/App")
                    feed_name = nav_text
                else:
                    # Fallback to using location and ICAO if no feed name found
                    feed_name = f"{location} ({icao})"
            
            # Create AirportFeed object
            feed = AirportFeed(
                icao=icao,
                location=location,
                status=status,
                listeners=listeners,
                total_listeners=total_listeners,
                channel_types=channel_types,
                metar=metar_text,
                frequencies=frequencies,
                timestamp=timestamp,
                feed_name=feed_name
            )
            
            feeds.append(feed)
            
        except Exception as e:
            print(f"Error processing feed: {e}")
    
    return feeds

def save_to_json(feeds, filename="liveatc_feeds.json"):
    # Load existing data if file exists
    existing_data = {}
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            try:
                existing_data = json.load(f)
            except json.JSONDecodeError:
                existing_data = {}
    
    # Update with new data
    for feed in feeds:
        feed_dict = feed.to_dict()
        feed_name = feed_dict['feed_name']
        
        if feed_name not in existing_data:
            # First time seeing this ICAO
            existing_data[feed_name] = {
                "static_data": {
                    "icao": feed_dict['icao'].upper(),
                    "location": feed_dict['location'],
                    "frequencies": feed_dict['frequencies'],
                    "channel_types": feed_dict['channel_types']
                },
                "time_series": []
            }
        
        # Add the time series data
        existing_data[feed_name]["time_series"].append({
            "timestamp": feed_dict['timestamp'],
            "status": feed_dict['status'],
            "listeners": feed_dict['listeners'],
            "total_listeners": feed_dict['total_listeners'],
            "metar": feed_dict['metar']
        })
    
    # Save updated data
    with open(filename, 'w') as f:
        json.dump(existing_data, f, indent=2)

async def main():
    # url = "https://www.liveatc.net/feedindex.php?type=international-af"
    # url = "https://www.liveatc.net/feedindex.php?type=hf"
    # url = "https://www.liveatc.net/feedindex.php?type=international-na"
    # urls = ["https://www.liveatc.net/feedindex.php?type=international-af", "https://www.liveatc.net/feedindex.php?type=international-af",
    #          "https://www.liveatc.net/feedindex.php?type=international-na"]
    urls = ["https://www.liveatc.net/feedindex.php?type=class-b", "https://www.liveatc.net/feedindex.php?type=class-c", 
            "https://www.liveatc.net/feedindex.php?type=class-d", "https://www.liveatc.net/feedindex.php?type=us-artcc",
            "https://www.liveatc.net/feedindex.php?type=canada", "https://www.liveatc.net/feedindex.php?type=international-eu", 
            "https://www.liveatc.net/feedindex.php?type=international-oc", "https://www.liveatc.net/feedindex.php?type=international-as",
            "https://www.liveatc.net/feedindex.php?type=international-sa", "https://www.liveatc.net/feedindex.php?type=international-na",
            "https://www.liveatc.net/feedindex.php?type=international-af", "https://www.liveatc.net/feedindex.php?type=hf"]
    
    for url in urls:
        # Create a ClientSession with SSL verification disabled
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            print(f"Now fetching {url} Please wait...")
            html = await fetch(session, url)
            # Wait 1 second after the page loads
            # await asyncio.sleep(1)
            
            # Extract feed data
            feeds = extract_feed_data(html)
            
            # Save to JSON
            save_to_json(feeds)
            
            print(f"Successfully processed {len(feeds)} feeds for {url}")

if __name__ == '__main__':
    asyncio.run(main())
