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
            channel_types = []
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
                # Find all text after the status
                metar_pattern = re.compile(r'[A-Z]{4}\s\d{6}Z.+?(?=<br />|$)')
                metar_match = metar_pattern.search(str(status_span))
                if metar_match:
                    metar_text = metar_match.group(0)
            
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
            frequencies = {}
            next_cell = row.find_next_sibling('td', {'valign': 'top'})
            if next_cell:
                freq_font = next_cell.find('font', {'class': 'purSep'})
                if freq_font:
                    freq_text = freq_font.text.strip()
                    for line in freq_text.split('\n'):
                        if ':' in line:
                            parts = line.split(':', 1)
                            if len(parts) == 2:
                                name, freq = parts
                                frequencies[name.strip()] = freq.strip()
            
            # Current timestamp in UTC
            timestamp = datetime.now(timezone.utc).isoformat()
            
            # Extract feed name from the strong tag text
            feed_name = "Unknown Feed"
            if strong_tag and strong_tag.find('a'):
                feed_name = strong_tag.find('a').text.strip()
            elif strong_tag:
                feed_name = strong_tag.text.strip()
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
                    "icao": feed_dict['icao'],
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
    url = "https://www.liveatc.net/feedindex.php?type=international-af"
    # url = "https://www.liveatc.net/feedindex.php?type=hf"
    # url = "https://www.liveatc.net/feedindex.php?type=international-na"

    # Create a ClientSession with SSL verification disabled
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        html = await fetch(session, url)
        # Wait 1 second after the page loads
        # await asyncio.sleep(1)
        
        # Extract feed data
        feeds = extract_feed_data(html)
        
        # Save to JSON
        save_to_json(feeds)
        
        print(f"Successfully processed {len(feeds)} feeds")

if __name__ == '__main__':
    asyncio.run(main())
