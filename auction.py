#!/usr/bin/env python3

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import math
from datetime import datetime, time as dt_time
import time
import random
import os
import pytz
from ib_logger import create_file_logger, LogPrintHelper
from ib_tg import send_telegram_message

# Set Mountain Time Zone
MOUNTAIN_TZ = pytz.timezone('America/Denver')

# Set up logging with Mountain Time
logger = create_file_logger('auction_world', '/var/www/favhome/auction_world.log')

# Configure logger to use Mountain Time
import logging
class MountainTimeFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, MOUNTAIN_TZ)
        if datefmt:
            return dt.strftime(datefmt)
        else:
            return dt.strftime('%Y-%m-%d %H:%M:%S MT')

# Update the logger formatter
for handler in logger.handlers:
    handler.setFormatter(MountainTimeFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

log_print = LogPrintHelper(logger, print_to_console=True)

EXCEL_FILE = '/var/www/favhome/auction_world.xlsx'

def get_mountain_time():
    """Get current Mountain Time"""
    return datetime.now(MOUNTAIN_TZ)

def format_mountain_time(dt=None):
    """Format datetime in Mountain Time"""
    if dt is None:
        dt = get_mountain_time()
    elif dt.tzinfo is None:
        # If naive datetime, assume it's UTC and convert
        dt = pytz.UTC.localize(dt).astimezone(MOUNTAIN_TZ)
    return dt.strftime('%Y-%m-%d %H:%M:%S MT')

def convert_sqft_to_acres(sqft_str):
    """Convert square feet to acres format"""
    try:
        # Extract numeric value from square feet string
        sqft_match = re.search(r'([\d,]+)\s*sq\.\s*ft\.', sqft_str, re.IGNORECASE)
        if sqft_match:
            sqft_value = int(sqft_match.group(1).replace(',', ''))
            # Convert to acres (1 acre = 43,560 sq ft)
            acres = sqft_value / 43560
            return f"{acres:.2f} acre(s)"
    except:
        pass
    return sqft_str

def normalize_lot_area(lot_area_text):
    """Normalize lot area to acres format, handling different input formats"""
    if not lot_area_text or lot_area_text == 'N/A':
        return lot_area_text
    
    # Already in acres format
    if 'acre' in lot_area_text.lower():
        return lot_area_text
    
    # Handle square feet format: "11,786 sq. ft.1,095 m2" -> convert to acres
    if 'sq. ft.' in lot_area_text.lower():
        return convert_sqft_to_acres(lot_area_text)
    
    # Handle other metric units - try to extract square feet if present
    sqft_pattern = r'([\d,]+)\s*sq\.?\s*ft\.?'
    sqft_match = re.search(sqft_pattern, lot_area_text, re.IGNORECASE)
    if sqft_match:
        return convert_sqft_to_acres(sqft_match.group(0))
    
    return lot_area_text

def clean_area_units(text):
    """Enhanced clean area text to keep only sq. ft. and acre(s), remove m2 and hectare(s)"""
    if not text or text == 'N/A':
        return text
    
    import re
    
    # For floor area: "4,431 sq. ft.412 m2" -> "4,431 sq. ft."
    text = re.sub(r'(\d+,?\d*\s*sq\.\s*ft\.)\d*\s*m2?', r'\1', text)
    
    # For lot area: "4.02 acre(s)1.63 hectare(s)" -> "4.02 acre(s)"
    text = re.sub(r'(\d+\.?\d*\s*acre\(s\))\d*\.?\d*\s*hectare\(s\)', r'\1', text)
    
    # Remove standalone metric units
    text = re.sub(r'\d+\.?\d*\s*m2\b', '', text)
    text = re.sub(r'\d+\.?\d*\s*hectare\(s\)', '', text)
    
    return text.strip()

def extract_coordinates_from_maps(soup, property_url, headers):
    """Extract latitude and longitude from embedded map data on the property page"""
    try:
        # Method 1: Look for coordinates in script tags on the main property page
        scripts = soup.find_all('script')
        
        # for script in scripts:
        #     if script.string:
        #         script_text = script.string

        for script in scripts:
            if script.string and ('lat' in script.string.lower() or 'lng' in script.string.lower()):
                # log_print.debug(f"Found script with coordinates: {script.string[:200]}...")
                script_text = script.string
                
                # # Look for latitude/longitude patterns in JavaScript
                # lat_pattern = r'(?:lat|latitude)["\']?\s*[:=]\s*([+-]?[45]\d\.?\d*)'
                # # lng_pattern = r'(?:lng|lon|longitude)["\']?\s*[:=]\s*([+-]?11\d\.?\d*)'
                # lng_pattern = r'(?:lng|lon|longitude)["\']?\s*[:=]\s*([+-]?1[01]\d\.?\d*)'

                # More flexible patterns for any decimal number
                lat_pattern = r'(?:lat|latitude)["\']?\s*[:=]\s*["\']?([+-]?\d+\.\d+)["\']?'
                lng_pattern = r'(?:lng|lon|longitude)["\']?\s*[:=]\s*["\']?([+-]?\d+\.\d+)["\']?'
                
                lat_match = re.search(lat_pattern, script_text, re.IGNORECASE)
                lng_match = re.search(lng_pattern, script_text, re.IGNORECASE)
                
                if lat_match and lng_match:
                    latitude = float(lat_match.group(1))
                    longitude = float(lng_match.group(1))
                    # Validate Calgary area coordinates
                    # if 50 <= latitude <= 52 and -116 <= longitude <= -112:    # for calgary only
                    if True:
                        log_print.info(f"Extracted coordinates from script: {latitude}, {longitude}")
                        return str(latitude), str(longitude)
        
        # Method 2: Look for data attributes in div elements
        divs_with_data = soup.find_all('div', attrs={'data-lat': True, 'data-lng': True})
        if divs_with_data:
            div = divs_with_data[0]
            latitude = div.get('data-lat')
            longitude = div.get('data-lng')
            if latitude and longitude:
                lat, lng = float(latitude), float(longitude)
                if 50 <= lat <= 52 and -116 <= lng <= -112:
                    log_print.info(f"Extracted coordinates from data attributes: {latitude}, {longitude}")
                    return str(latitude), str(longitude)
        
        log_print.debug(f"Could not extract coordinates from property page: {property_url}")
        return 'N/A', 'N/A'
        
    except Exception as e:
        log_print.error(f"Error extracting coordinates: {e}")
        return 'N/A', 'N/A'

def calculate_driving_time_osrm(origin_lat, origin_lng, dest_lat, dest_lng):
    """Calculate driving time between two coordinates using free OSRM service"""
    try:
        # OSRM public demo server (completely free, no API key needed)
        base_url = "http://router.project-osrm.org/route/v1/driving"
        url = f"{base_url}/{origin_lng},{origin_lat};{dest_lng},{dest_lat}"
        
        params = {
            'overview': 'false',
            'alternatives': 'false',
            'steps': 'false',
            'geometries': 'geojson'
        }
        
        # Make the API request
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if data['code'] == 'Ok' and data['routes']:
            route = data['routes'][0]
            duration_seconds = route['duration']
            distance_meters = route['distance']
            
            # Convert to readable format
            duration_minutes = int(duration_seconds / 60)
            distance_km = distance_meters / 1000
            
            log_print.info(f"Calculated driving time: {duration_minutes} mins, distance: {distance_km:.1f} km")
            return duration_minutes, distance_km
        else:
            log_print.warning(f"OSRM routing failed: {data.get('code', 'Unknown error')}")
            return None, None
            
    except Exception as e:
        log_print.error(f"Error calculating driving time: {e}")
        return None, None

def calculate_direction(origin_lat, origin_lng, dest_lat, dest_lng):
    """Calculate the compass direction from origin to destination"""
    try:
        # Convert degrees to radians
        lat1 = math.radians(origin_lat)
        lat2 = math.radians(dest_lat)
        delta_lng = math.radians(dest_lng - origin_lng)
        
        # Calculate bearing using the formula
        y = math.sin(delta_lng) * math.cos(lat2)
        x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(delta_lng)
        
        # Get bearing in radians, then convert to degrees
        bearing_rad = math.atan2(y, x)
        bearing_deg = math.degrees(bearing_rad)
        
        # Normalize to 0-360 degrees
        bearing_deg = (bearing_deg + 360) % 360
        
        # Convert to 8-point compass direction
        # The ranges are: N(337.5-22.5), NE(22.5-67.5), E(67.5-112.5), SE(112.5-157.5), 
        # S(157.5-202.5), SW(202.5-247.5), W(247.5-292.5), NW(292.5-337.5)
        if bearing_deg >= 337.5 or bearing_deg < 22.5:
            direction = "North"
        elif bearing_deg < 67.5:
            direction = "Northeast"
        elif bearing_deg < 112.5:
            direction = "East"
        elif bearing_deg < 157.5:
            direction = "Southeast"
        elif bearing_deg < 202.5:
            direction = "South"
        elif bearing_deg < 247.5:
            direction = "Southwest"
        elif bearing_deg < 292.5:
            direction = "West"
        else:  # 292.5 <= bearing_deg < 337.5
            direction = "Northwest"
        
        log_print.info(f"Calculated direction: {direction} (bearing: {bearing_deg:.1f}°)")
        return direction
        
    except Exception as e:
        log_print.error(f"Error calculating direction: {e}")
        return 'N/A'

def fetch_detailed_property_info(property_url):
    """Fetch detailed property information from a property page"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(property_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        log_print.info(f"Successfully fetched detailed property page: {property_url}")
        
        details = {}
        
        # Extract Year Built from General Info section
        year_built = extract_field_from_section(soup, "Year built:")
        details['Year Built'] = year_built
        
        # Extract General Info
        general_info = extract_section_data(soup, "General Info:")
        details['General Info'] = general_info
        
        # Extract Additional Info
        additional_info = extract_section_data(soup, "Additional Info:")
        details['Additional Info'] = additional_info
        
        # Extract Community
        community = extract_section_data(soup, "Community:")
        details['Community'] = community

        # Extract Appliances
        appliances = extract_section_data(soup, "Appliances:")
        details['Appliances'] = appliances

        # Extract Inclusions
        inclusions = extract_section_data(soup, "Inclusions:")
        details['Inclusions'] = inclusions
        
        # Extract Restrictions
        restrictions = extract_section_data(soup, "Restrictions:")
        details['Restrictions'] = restrictions

        # Extract Restrictions
        listing_info = extract_section_data(soup, "Listing Info:")
        details['Listing Info'] = listing_info

        # Extract Land Info
        land_info = extract_section_data(soup, "Land Info:")
        details['Land Info'] = land_info
        
        # Extract coordinates from "more maps" link
        latitude, longitude = extract_coordinates_from_maps(soup, property_url, headers)
        details['Latitude'] = latitude
        details['Longitude'] = longitude
        
        return details
        
    except Exception as e:
        log_print.error(f"Error fetching detailed property info from {property_url}: {e}")
        return {}

def extract_field_from_section(soup, field_name):
    """Extract a specific field value from the page"""
    try:
        field_element = soup.find(string=lambda text: text and field_name in text)
        if field_element:
            parent = field_element.parent
            if parent:
                next_text = parent.find_next_sibling()
                if next_text:
                    return next_text.get_text(strip=True)
                else:
                    full_text = parent.get_text(strip=True)
                    if field_name in full_text:
                        return full_text.replace(field_name, '').strip()
        return 'N/A'
    except Exception as e:
        log_print.error(f"Error extracting field {field_name}: {e}")
        return 'N/A'

def extract_section_data(soup, section_title):
    """Extract all data from a specific section"""
    try:
        section_header = soup.find(string=lambda text: text and section_title in text)
        if not section_header:
            return 'N/A'
        
        section_container = section_header.parent
        if not section_container:
            return 'N/A'
        
        data_elements = []
        current = section_container.find_next_sibling()
        
        while current and not is_next_section(current):
            text = current.get_text(strip=True)
            if text and text not in ['', section_title]:
                data_elements.append(text)
            current = current.find_next_sibling()
        
        container_text = section_container.get_text(strip=True)
        if container_text and section_title in container_text:
            after_title = container_text.split(section_title, 1)
            if len(after_title) > 1:
                clean_text = after_title[1].strip()
                if clean_text:
                    data_elements.append(clean_text)
        
        if data_elements:
            return ' | '.join(data_elements)
        else:
            return 'N/A'
            
    except Exception as e:
        log_print.error(f"Error extracting section {section_title}: {e}")
        return 'N/A'

def is_next_section(element):
    """Check if this element is the start of a new section"""
    text = element.get_text(strip=True)
    section_headers = [
        'General Info:', 'Additional Info:', 'Community:', 'Appliances:', 
        'Inclusions:', 'Restrictions:', 'Land Info:', 'Listing Info:',
        'Room Information:', 'Land Info:', 'Listing Info:'
    ]
    return any(header in text for header in section_headers)

def parse_detailed_sections(detailed_info):
    """Parse detailed information sections into separate columns"""
    parsed_data = {}
    
    for section_name, section_data in detailed_info.items():
        if section_data == 'N/A' or not section_data:
            continue
            
        if section_name == 'Year Built':
            year_value = section_data
            if '(' in year_value:
                year_value = year_value.split('(')[0].strip()
            parsed_data['Year_Built'] = year_value
            continue
        
        # Handle coordinate fields directly
        if section_name in ['Latitude', 'Longitude']:
            parsed_data[section_name] = section_data
            continue
            
        if '|' in section_data:
            items = section_data.split('|')
            for item in items:
                item = item.strip()
                if ':' in item:
                    key, value = item.split(':', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # Enhanced lot area handling
                    # if 'lot area' in key.lower() or 'area' in key.lower():
                    if 'lot area' in key.lower():
                        value = normalize_lot_area(clean_area_units(value))
                    else:
                        value = clean_area_units(value)
                    
                    column_name = f"{section_name}_{key}".replace(' ', '_').replace('.', '')
                    parsed_data[column_name] = value
        else:
            if ':' in section_data:
                key, value = section_data.split(':', 1)
                key = key.strip()
                value = value.strip()
                
                # Enhanced lot area handling
                if 'lot area' in key.lower() or 'area' in key.lower():
                    value = normalize_lot_area(clean_area_units(value))
                else:
                    value = clean_area_units(value)
                
                column_name = f"{section_name}_{key}".replace(' ', '_').replace('.', '')
                parsed_data[column_name] = value
            else:
                column_name = section_name.replace(' ', '_').replace('.', '')
                
                # Enhanced lot area handling for section data
                if 'lot' in section_name.lower() or 'area' in section_name.lower():
                    section_data = normalize_lot_area(clean_area_units(section_data))
                else:
                    section_data = clean_area_units(section_data)
                
                parsed_data[column_name] = section_data
    
    return parsed_data

def extract_property_details(property_div, existing_df=None):
    """Extract property details from a property div element"""
    try:
        # Extract property title and address
        title_element = property_div.find('h3')
        title = title_element.get_text(strip=True) if title_element else 'N/A'
        
        # Extract MLS number from title
        mls_match = re.search(r'MLS®#\s*([A-Z0-9]+)', title)
        mls_number = mls_match.group(1) if mls_match else 'N/A'
        
        # Check if this MLS already exists in Excel to reuse location data
        existing_location_data = {}
        if existing_df is not None and not existing_df.empty and mls_number != 'N/A':
            existing_property = existing_df[existing_df['MLS Number'].astype(str) == mls_number]
            if not existing_property.empty:
                log_print.info(f"MLS {mls_number} already exists in Excel, reusing location data")
                existing_location_data = {
                    'Latitude': existing_property.iloc[0].get('Latitude', 'N/A'),
                    'Longitude': existing_property.iloc[0].get('Longitude', 'N/A'),
                    'Distance_KM': existing_property.iloc[0].get('Distance_KM', 'N/A'),
                    'Driving_Time_Min': existing_property.iloc[0].get('Driving_Time_Min', 'N/A'),
                    'Direction': existing_property.iloc[0].get('Direction', 'N/A')
                }
        
        # Extract address (part before the colon in title)
        address = title.split(':')[0].strip() if ':' in title else 'N/A'
        
        # Extract location (part after first colon, before "for sale")
        location_match = re.search(r':\s*(.+?)\s+(?:Detached\s+)?for\s+sale', title)
        location = location_match.group(1).strip() if location_match else 'N/A'
        
        # Extract price - look for price container
        price = 'N/A'
        price_container = property_div.find('div', class_='mrp-listing-price-container')
        if price_container:
            price_text = price_container.get_text(strip=True)
            if price_text.startswith('$'):
                price = price_text
        
        # Extract property details (bedrooms, bathrooms, floor area)
        bedrooms = 'N/A'
        bathrooms = 'N/A'
        floor_area = 'N/A'
        
        # Extract bedrooms
        bedrooms_dd = property_div.find('dd', class_='bedrooms-line')
        if bedrooms_dd:
            bedrooms_span = bedrooms_dd.find('span')
            if bedrooms_span:
                bedrooms = bedrooms_span.get_text(strip=True)
        
        # Extract bathrooms
        bathrooms_dd = property_div.find('dd', class_='bathrooms-line')
        if bathrooms_dd:
            bathrooms_span = bathrooms_dd.find('span')
            if bathrooms_span:
                bathrooms = bathrooms_span.get_text(strip=True)
        
        # Extract floor area
        floor_area_dd = property_div.find('dd', class_='floor-area-line')
        if floor_area_dd:
            floor_area_span = floor_area_dd.find('span', class_='mrp-i-unit')
            if floor_area_span:
                floor_area = clean_area_units(floor_area_span.get_text(strip=True))
        
        # Extract image URL - look for the main property image
        image_url = 'N/A'
        img_element = property_div.find('img', class_='mrp-listing-main-image')
        if img_element:
            src = img_element.get('data-src') or img_element.get('src')
            if src and not src.startswith('data:'):
                if src.startswith('//'):
                    image_url = 'https:' + src
                elif src.startswith('/'):
                    image_url = 'https://dianerichardson.ca' + src
                elif src.startswith('http'):
                    image_url = src
                else:
                    image_url = 'https://dianerichardson.ca/' + src
        
        # Extract property link
        property_link = 'N/A'
        listing_links = property_div.find_all('a', href=re.compile(r'listing\.'))
        if listing_links:
            href = listing_links[0].get('href')
            print(href)
            if href.startswith('/'):
                property_link = 'https://dianerichardson.ca' + href
            elif href.startswith('http'):
                property_link = href
            else:
                property_link = 'https://dianerichardson.ca/' + href
        
        # Extract description
        description = 'N/A'
        description_div = property_div.find('div', class_='mrp-listing-description')
        if description_div:
            description_span = description_div.find('span', class_='inner')
            if description_span:
                description_text = description_span.get_text(strip=True)
                description = description_text[:500] + '...' if len(description_text) > 500 else description_text
        
        if description == 'N/A':
            description_text = property_div.get_text()
            sentences = description_text.split('.')
            description = '. '.join(sentences[:3]).strip() if len(sentences) >= 3 else description_text[:200]
        
        # Base property info (with location columns after Location)
        property_info = {
            'Title': title,
            'Address': address,
            'Location': location,
            'Latitude': existing_location_data.get('Latitude', 'N/A'),
            'Longitude': existing_location_data.get('Longitude', 'N/A'),
            'Distance_KM': existing_location_data.get('Distance_KM', 'N/A'),
            'Driving_Time_Min': existing_location_data.get('Driving_Time_Min', 'N/A'),
            'Direction': existing_location_data.get('Direction', 'N/A'),
            'MLS Number': mls_number,
            'Price': price,
            'Bedrooms': bedrooms,
            'Bathrooms': bathrooms,
            'Floor Area': floor_area,
            'Image URL': image_url,
            'Property Link': property_link,
            'Description': description,
            'Listing Date': get_mountain_time().strftime('%Y-%m-%d'),
            'Discovery Time': format_mountain_time()
        }
        
        # Fetch detailed information if property link is available
        if property_link != 'N/A':
            log_print.info(f"Fetching detailed info for {mls_number}")
            detailed_info = fetch_detailed_property_info(property_link)
            parsed_details = parse_detailed_sections(detailed_info)
            property_info.update(parsed_details)
            
            # Calculate location data if we need any missing information
            need_coordinates = property_info['Latitude'] == 'N/A' or property_info['Longitude'] == 'N/A'
            need_location_calc = (property_info['Distance_KM'] == 'N/A' or 
                                property_info['Driving_Time_Min'] == 'N/A' or 
                                property_info['Direction'] == 'N/A')
            
            # Get coordinates from detailed info if we don't have them
            if need_coordinates and detailed_info and detailed_info.get('Latitude', 'N/A') != 'N/A' and detailed_info.get('Longitude', 'N/A') != 'N/A':
                log_print.info(f"Extracting coordinates for {mls_number}")
                property_info['Latitude'] = detailed_info['Latitude']
                property_info['Longitude'] = detailed_info['Longitude']
                need_location_calc = True  # Force calculation since we have new coordinates
            
            # Calculate driving time, distance and direction if needed and we have coordinates
            if need_location_calc and property_info['Latitude'] != 'N/A' and property_info['Longitude'] != 'N/A':
                log_print.info(f"Calculating location data for {mls_number}")
                latitude = property_info['Latitude']
                longitude = property_info['Longitude']
                
                # Calculate driving time, distance and direction
                ref_lat, ref_lng = 51.060428, -114.180062
                prop_lat, prop_lng = float(latitude), float(longitude)
                
                # Calculate driving time and distance using OSRM
                driving_time_min, distance_km = calculate_driving_time_osrm(ref_lat, ref_lng, prop_lat, prop_lng)
                
                if driving_time_min is not None and distance_km is not None:
                    property_info['Driving_Time_Min'] = driving_time_min
                    property_info['Distance_KM'] = distance_km
                    
                    # Calculate direction from reference point to property
                    direction = calculate_direction(ref_lat, ref_lng, prop_lat, prop_lng)
                    property_info['Direction'] = direction
            elif property_info['Distance_KM'] != 'N/A' and property_info['Driving_Time_Min'] != 'N/A' and property_info['Direction'] != 'N/A':
                log_print.info(f"Using complete existing location data for {mls_number}")
            else:
                log_print.warning(f"Could not calculate complete location data for {mls_number}")
            
            # Be respectful to the server with random sleep between 25-35 seconds
            sleep_time = random.randint(25, 35)
            # log_print.info(f"Sleeping for {sleep_time} seconds before next property...")
            time.sleep(sleep_time)
        
        # Calculate price per acre after getting lot area info
        lot_area_for_calc = property_info.get('General_Info_Lot_Area', 
                           property_info.get('Land_Info_Lot_Area', 'N/A'))
        if lot_area_for_calc == 'N/A':
            lot_area_for_calc = property_info.get('Floor Area', 'N/A')  # Fallback
        
        price_per_acre = calculate_price_per_acre(property_info.get('Price', 'N/A'), lot_area_for_calc)
        property_info['Price_Per_Acre'] = price_per_acre
        
        # Calculate price per square foot
        price_per_sqft = calculate_price_per_sqft(property_info.get('Price', 'N/A'), property_info.get('Floor Area', 'N/A'))
        property_info['Price_Per_SqFt'] = price_per_sqft
        
        return property_info
        
    except Exception as e:
        log_print.error(f"Error extracting property details: {e}")
        return {}

def load_existing_properties(property_category=None):
    """Load existing properties from Excel file and get the most recent MLS number for a specific category"""
    try:
        if os.path.exists(EXCEL_FILE):
            df = pd.read_excel(EXCEL_FILE)
            if not df.empty and 'MLS Number' in df.columns:
                # Filter by property category if specified
                if property_category and 'Property Category' in df.columns:
                    category_df = df[df['Property Category'] == property_category]
                    if not category_df.empty:
                        # Get the most recent MLS number for this category
                        most_recent_mls = str(category_df.iloc[0]['MLS Number'])
                        log_print.info(f"Loaded Excel with {len(df)} total properties, {len(category_df)} in category '{property_category}'. Most recent MLS for category: {most_recent_mls}")
                        return df, most_recent_mls  # Return full df to preserve all categories
                    else:
                        log_print.info(f"No properties found for category '{property_category}'")
                        return df, None  # Return full df, no recent MLS for this category
                else:
                    # No category specified or column doesn't exist, use original logic
                    most_recent_mls = str(df.iloc[0]['MLS Number'])
                    log_print.info(f"Loaded Excel with {len(df)} properties. Most recent MLS: {most_recent_mls}")
                    return df, most_recent_mls
            else:
                log_print.info("Excel file exists but is empty or missing MLS column")
                return pd.DataFrame(), None
        else:
            log_print.info("Excel file doesn't exist, starting fresh")
            return pd.DataFrame(), None
    except Exception as e:
        log_print.error(f"Error loading existing properties: {e}")
        return pd.DataFrame(), None

def save_properties_to_excel(new_properties, existing_df, property_category):
    """Save new properties to Excel, adding them at the top while preserving other categories"""
    try:
        if new_properties:
            # Add property_category to all new properties
            for prop in new_properties:
                prop['Property Category'] = property_category
            
            new_df = pd.DataFrame(new_properties)
            
            if not existing_df.empty:
                # Filter existing properties to get those from the same category
                if 'Property Category' in existing_df.columns:
                    same_category_df = existing_df[existing_df['Property Category'] == property_category]
                    other_categories_df = existing_df[existing_df['Property Category'] != property_category]
                else:
                    # If Property Category column doesn't exist, treat all existing as other categories
                    same_category_df = pd.DataFrame()
                    other_categories_df = existing_df
                
                # Combine: new properties + existing same category + other categories
                if not same_category_df.empty:
                    combined_same_category = pd.concat([new_df, same_category_df], ignore_index=True)
                else:
                    combined_same_category = new_df
                
                if not other_categories_df.empty:
                    combined_df = pd.concat([combined_same_category, other_categories_df], ignore_index=True)
                else:
                    combined_df = combined_same_category
            else:
                combined_df = new_df
            
            # Save to Excel
            combined_df.to_excel(EXCEL_FILE, index=False)
            log_print.info(f"Saved {len(new_properties)} new '{property_category}' properties to Excel (total: {len(combined_df)})")
            return True
        return False
    except Exception as e:
        log_print.error(f"Error saving properties to Excel: {e}")
        return False

def calculate_price_per_acre(price_str, lot_area_str):
    """Calculate price per acre from price and lot area strings"""
    try:
        # Extract price value
        price_match = re.search(r'\$?([\d,]+)', price_str.replace('$', '').replace(',', ''))
        if not price_match:
            return 'N/A'
        
        price_value = int(price_match.group(1).replace(',', ''))
        
        # Extract lot area in acres
        if 'acre' in lot_area_str.lower():
            acre_match = re.search(r'([\d.]+)\s*acre', lot_area_str)
            if acre_match:
                acres = float(acre_match.group(1))
                if acres > 0:
                    price_per_acre = price_value / acres
                    return f"${price_per_acre:,.0f}/acre"
        
        return 'N/A'
    except:
        return 'N/A'

def calculate_price_per_sqft(price_str, floor_area_str):
    """Calculate price per square foot from price and floor area strings"""
    try:
        # Extract price value
        price_match = re.search(r'\$?([\d,]+)', price_str.replace('$', '').replace(',', ''))
        if not price_match:
            return 'N/A'
        
        price_value = int(price_match.group(1).replace(',', ''))
        
        # Extract floor area in square feet
        if 'sq. ft.' in floor_area_str.lower() or 'sq ft' in floor_area_str.lower():
            sqft_match = re.search(r'([\d,]+)\s*sq\.?\s*ft\.?', floor_area_str)
            if sqft_match:
                sqft = int(sqft_match.group(1).replace(',', ''))
                if sqft > 0:
                    price_per_sqft = price_value / sqft
                    return f"${price_per_sqft:,.0f}/sqft"
        
        return 'N/A'
    except:
        return 'N/A'

def format_telegram_message(property_info, category=None):
    """Format property information for Telegram message"""
    try:
        mls = property_info.get('MLS Number', 'N/A')
        address = property_info.get('Address', 'N/A')
        location = property_info.get('Location', 'N/A')
        price = property_info.get('Price', 'N/A')
        bedrooms = property_info.get('Bedrooms', 'N/A')
        bathrooms = property_info.get('Bathrooms', 'N/A')
        floor_area = property_info.get('Floor Area', 'N/A')
        
        # Get lot area from detailed info (prioritize General_Info_Lot_Area)
        lot_area = property_info.get('General_Info_Lot_Area', 
                  property_info.get('Land_Info_Lot_Area', 'N/A'))
        
        if float(lot_area.split(' ')[0]) < 1:
            lot_area = f"{int(float(lot_area.split(' ')[0]) * 43560)} sq. ft."

        year_built = property_info.get('Year_Built', 'N/A')
        property_link = property_info.get('Property Link', 'N/A')
        image_url = property_info.get('Image URL', 'N/A')
        
        # Get coordinates and driving info
        latitude = property_info.get('Latitude', 'N/A')
        longitude = property_info.get('Longitude', 'N/A')
        distance_km = property_info.get('Distance_KM', 'N/A')
        driving_time_min = property_info.get('Driving_Time_Min', 'N/A')
        direction = property_info.get('Direction', 'N/A')
        
        # Format driving time - prefer stored values, calculate if missing
        driving_time = 'N/A'
        try:
            time_val = int(float(driving_time_min))
            dist_val = float(distance_km)
            if direction != 'N/A':
                driving_time = f"{time_val} mins ({dist_val:.1f} km {direction})"
            else:
                driving_time = f"{time_val} mins ({dist_val:.1f} km)"
        except:
            pass
        
        # Add driving time info if available
        driving_info = ""
        if driving_time != 'N/A':
            driving_info = f"\n🚗 <b>Drive Time:</b> {driving_time}"

        # Calculate price per acre
        price_per_acre = property_info.get('Price_Per_Acre', 'N/A')
        
        # Calculate price per square foot
        price_per_sqft = property_info.get('Price_Per_SqFt', 'N/A')
        
        # Determine title based on category
        if category == 'alberta-acreages':
            title_prefix = f"NEW ACREAGE PROPERTY! ({price_per_acre})"
            emoji = "🌾"
        elif category == 'calgary-single-family':
            title_prefix = f"NEW SINGLE FAMILY HOUSE! ({price_per_sqft})"
            emoji = "🏠"
        elif category == 'walkout-basement':
            title_prefix = f"NEW WALKOUT HOUSE! ({price_per_sqft})"
            emoji = "🚪"
        elif category == 'price-reduced':
            title_prefix = f"NEW PRICE REDUCED PROPERTY! ({price_per_sqft})"
            emoji = "⬇️"
        else:
            title_prefix = "NEW PROPERTY!"
            emoji = "🏠"
        
        # Create clickable title with embedded link
        if property_link != 'N/A':
            title = f'{emoji} <b><a href="{property_link}">{title_prefix}</a></b> {emoji}'
        else:
            title = f"{emoji} <b>{title_prefix}</b> {emoji}"
        
        # Create Google Maps link if coordinates are available
        maps_link = ""
        if latitude != 'N/A' and longitude != 'N/A':
            google_maps_url = f"https://maps.google.com/?q={latitude},{longitude}"
            maps_link = f"\n📍 <a href=\"{google_maps_url}\">View on Google Maps</a>"
        
        message = f"""{title}

📍 <b>Address:</b> {address}
🏘️ <b>Location:</b> {location}{driving_info}
💰 <b>Price:</b> {price}
🌾 <b>Lot Area:</b> {lot_area}
💵 <b>Price/Acre:</b> {price_per_acre}
📐 <b>Floor Area:</b> {floor_area}
💲 <b>Price/SqFt:</b> {price_per_sqft}

🛏️ <b>Bedrooms:</b> {bedrooms}
🚿 <b>Bathrooms:</b> {bathrooms}
🏗️ <b>Year Built:</b> {year_built}
🏷️ <b>MLS #:</b> {mls}{maps_link}

⏰ <i>Discovered: {property_info.get('Discovery Time', 'N/A')}</i>"""
        
        return message
    except Exception as e:
        log_print.error(f"Error formatting Telegram message: {e}")
        return f"New property found: MLS {property_info.get('MLS Number', 'N/A')} - {property_info.get('Address', 'N/A')}"

def check_for_new_properties():
    """Check for new properties from multiple URLs and send notifications"""
    try:
        log_print.info("Starting property check...")
        
        # Define URLs and their corresponding property categories
        url_categories = [
            {
                'url': 'https://dianerichardson.ca/alberta-acreages-for-sale.html?sort=2',
                'category': 'alberta-acreages'
            },
            {
                'url': 'https://dianerichardson.ca/calgary-real-estate.html?sort=2',
                'category': 'calgary-single-family'
            },
            {
                'url': 'https://dianerichardson.ca/walkout-basement-homes-for-sale-in-calgary.html?sort=2',
                'category': 'walkout-basement'
            },
            {
                'url': 'https://dianerichardson.ca/calgary-homes-with-recent-price-reductions.html?sort=2',
                'category': 'price-reduced'
            },
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        total_new_properties = 0
        
        # Process each URL
        for url_config in url_categories:
            url = url_config['url']
            property_category = url_config['category']
            
            log_print.info(f"Checking URL: {url} for category: {property_category}")
            
            # Load existing properties and get most recent MLS for this category
            existing_df, most_recent_mls = load_existing_properties(property_category)
            
            try:

                property_sections = []
                for page in range(1, 6):
                    if page == 1:
                        page_url = url
                    else:
                        if '?' in url:
                            base_url = url.split('?')[0]
                            page_url = f"{base_url}?_pg={page}"
                        else:
                            page_url = f"{url}?_pg={page}"
                    # print(page_url)
                    response = requests.get(page_url, headers=headers, timeout=30)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')
                    property_sections.extend(soup.find_all('li', class_='Auction Lots Listing'))
                    time.sleep(random.randint(25, 35))

                # response = requests.get(url, headers=headers, timeout=30)
                # response.raise_for_status()
                
                # soup = BeautifulSoup(response.content, 'html.parser')
                # property_sections = soup.find_all('li', class_='Auction Lots Listing')
                
                log_print.info(f"Found {len(property_sections)} properties for category '{property_category}'")
                
                new_properties = []
                found_recent_mls = False
                
                # Check each property in order (newest first on the page)
                for property_container in property_sections:
                    try:
                        # Quick MLS check first
                        title_element = property_container.find('h3')
                        if title_element:
                            title = title_element.get_text(strip=True)
                            mls_match = re.search(r'MLS®#\s*([A-Z0-9]+)', title)
                            if mls_match:
                                mls_number = mls_match.group(1)
                                
                                # If this is the most recent MLS from our Excel for this category, stop here
                                if most_recent_mls and mls_number == most_recent_mls:
                                    log_print.info(f"Found MLS {mls_number} for category '{property_category}' - continue to the next property")
                                    found_recent_mls = True
                                    continue
                                
                                # This is a new property (appears before our most recent MLS)
                                log_print.info(f"New property found: MLS {mls_number} (category: {property_category})")
                                
                                # Extract full details for new property, passing existing_df to check for reusing location data
                                property_data = extract_property_details(property_container, existing_df)
                                if property_data:
                                    new_properties.append(property_data)
                                    
                                    # Send Telegram notification
                                    telegram_message = format_telegram_message(property_data, property_category)
                                    send_telegram_message(
                                        message=telegram_message,
                                        parse_mode='HTML',
                                        caller_logger=log_print
                                    )
                                    
                                    log_print.info(f"Sent Telegram notification for MLS {mls_number}")
                                    
                    except Exception as e:
                        log_print.error(f"Error processing property container: {e}")
                        continue
                
                # If we didn't find our most recent MLS, it might mean there are many new properties
                # or the previous property was removed. In this case, we still process what we found.
                if most_recent_mls and not found_recent_mls:
                    log_print.warning(f"Did not find most recent MLS {most_recent_mls} for category '{property_category}' on first page. May have many new properties or property was removed.")
                
                # Save new properties if any found (they will be added at the top)
                if new_properties:
                    # Don't reverse - keep the order so the newest property (first on webpage) goes to top of Excel
                    # new_properties is already in the correct order: newest first
                    
                    if save_properties_to_excel(new_properties, existing_df, property_category):
                        log_print.info(f"Successfully processed {len(new_properties)} new properties for category '{property_category}'")
                        total_new_properties += len(new_properties)
                else:
                    log_print.info(f"No new properties found for category '{property_category}'")
                
            except Exception as e:
                log_print.error(f"Error fetching properties from {url}: {e}")
                continue
        
        log_print.info(f"Property check completed. Total new properties found: {total_new_properties}")
        return total_new_properties
        
    except Exception as e:
        log_print.error(f"Error checking for new properties: {e}")
        error_message = f"❌ <b>Property Check Error</b>\n\n{str(e)}\n⏰ {format_mountain_time()}"
        send_telegram_message(
            message=error_message,
            parse_mode='HTML',
            caller_logger=log_print
        )
        return 0

def get_random_check_time():
    """Get random time for next check (40-80 minutes from now)"""
    # Random minutes between 40 and 80
    random_minutes = random.randint(40, 80)
    return random_minutes

def run_continuous_monitor():
    """Run continuous monitoring with random schedule"""
    log_print.info("Starting continuous property monitoring...")
    
    # Send startup notification
    startup_message = f"🤖 <b>Alberta Acreage Monitor Started</b>\n\n📅 Schedule: Random checks between 6 AM - 12 AM MT\n⏰ Started: {format_mountain_time()}"
    send_telegram_message(
        message=startup_message,
        parse_mode='HTML',
        caller_logger=log_print
    )
    
    while True:
        try:
            current_mountain_time = get_mountain_time()
            current_time = current_mountain_time.time()
            
            # Check if we're in operating hours (6 AM to 12 AM Mountain Time)
            if dt_time(6, 0) <= current_time <= dt_time(23, 59):
                # Perform property check
                new_count = check_for_new_properties()
                log_print.info(f"Property check completed. Found {new_count} new properties.")

                # Get random minutes for next check (40-80 minutes)
                random_minutes = get_random_check_time()
                
                # Calculate next check time (current time + random minutes)
                now_mt = current_mountain_time
                from datetime import timedelta
                next_check_datetime = now_mt + timedelta(minutes=random_minutes)
                
                # If next check goes past midnight, schedule for 6 AM tomorrow
                if next_check_datetime.time() < dt_time(6, 0) or next_check_datetime.time() > dt_time(23, 59):
                    # Schedule for 6 AM tomorrow + random minutes
                    tomorrow_6am = datetime.combine(now_mt.date() + timedelta(days=1), dt_time(6, 0))
                    tomorrow_6am = MOUNTAIN_TZ.localize(tomorrow_6am)
                    next_check_datetime = tomorrow_6am + timedelta(minutes=random.randint(0, 60))
                
                sleep_seconds = (next_check_datetime - now_mt).total_seconds()
                
                log_print.info(f"Next property check scheduled for: {next_check_datetime.strftime('%Y-%m-%d %H:%M:%S MT')}")
                log_print.info(f"Sleeping for {random_minutes} minutes ({sleep_seconds/60:.1f} minutes)...")
                
                time.sleep(sleep_seconds)
                
            else:
                # Outside operating hours, sleep until 6 AM MT
                now_mt = current_mountain_time
                if current_time < dt_time(6, 0):
                    # Before 6 AM today
                    tomorrow_6am = datetime.combine(now_mt.date(), dt_time(6, 0))
                else:
                    # After midnight, sleep until 6 AM tomorrow
                    from datetime import timedelta
                    tomorrow_6am = datetime.combine(now_mt.date() + timedelta(days=1), dt_time(6, 0))
                
                tomorrow_6am = MOUNTAIN_TZ.localize(tomorrow_6am)
                sleep_seconds = (tomorrow_6am - now_mt).total_seconds()
                
                log_print.info(f"Outside operating hours. Sleeping until 6 AM MT ({sleep_seconds/3600:.1f} hours)")
                time.sleep(sleep_seconds)
                
        except KeyboardInterrupt:
            log_print.info("Property monitoring stopped by user")
            break
        except Exception as e:
            log_print.error(f"Error in continuous monitoring: {e}")
            # Sleep for 1 hour before retrying
            time.sleep(3600)

def test_with_existing_property():
    """For testing: get the most recent property from Excel and send as if it's new"""
    try:
        log_print.info("No new properties found. Testing with most recent property from Excel...")
        
        # Load existing properties (all categories)
        existing_df, most_recent_mls = load_existing_properties()
        
        if existing_df.empty:
            log_print.info("No properties in Excel file to test with.")
            return False
        
        # Get the top (most recent) property
        top_property = existing_df.iloc[0].to_dict()
        
        # Convert to the format expected by format_telegram_message
        property_info = {
            'MLS Number': str(top_property.get('MLS Number', 'N/A')),
            'Address': str(top_property.get('Address', 'N/A')),
            'Location': str(top_property.get('Location', 'N/A')),
            'Latitude': str(top_property.get('Latitude', 'N/A')),
            'Longitude': str(top_property.get('Longitude', 'N/A')),
            'Distance_KM': str(top_property.get('Distance_KM', 'N/A')),
            'Driving_Time_Min': str(top_property.get('Driving_Time_Min', 'N/A')),
            'Direction': str(top_property.get('Direction', 'N/A')),
            'Price': str(top_property.get('Price', 'N/A')),
            'Bedrooms': str(top_property.get('Bedrooms', 'N/A')),
            'Bathrooms': str(top_property.get('Bathrooms', 'N/A')),
            'Floor Area': str(top_property.get('Floor Area', 'N/A')),
            'General_Info_Lot_Area': str(top_property.get('General_Info_Lot_Area', 'N/A')),
            'Land_Info_Lot_Area': str(top_property.get('Land_Info_Lot_Area', 'N/A')),
            'Year_Built': str(top_property.get('Year_Built', 'N/A')),
            'Property Link': str(top_property.get('Property Link', 'N/A')),
            'Image URL': str(top_property.get('Image URL', 'N/A')),
            'Price_Per_Acre': str(top_property.get('Price_Per_Acre', 'N/A')),
            'Price_Per_SqFt': str(top_property.get('Price_Per_SqFt', 'N/A')),
            'Discovery Time': format_mountain_time()  # Use current time for testing
        }
        
        # Clean up any 'nan' values and handle pandas NaN
        import pandas as pd
        for key, value in property_info.items():
            if value == 'nan' or value == 'None' or pd.isna(value):
                property_info[key] = 'N/A'
        
        # If coordinates are missing, try to fetch them from the property page
        if (property_info['Latitude'] == 'N/A' or property_info['Longitude'] == 'N/A') and property_info['Property Link'] != 'N/A':
            log_print.info(f"Coordinates missing for test property, attempting to fetch from property page...")
            try:
                detailed_info = fetch_detailed_property_info(property_info['Property Link'])
                if detailed_info.get('Latitude', 'N/A') != 'N/A' and detailed_info.get('Longitude', 'N/A') != 'N/A':
                    property_info['Latitude'] = detailed_info['Latitude']
                    property_info['Longitude'] = detailed_info['Longitude']
                    log_print.info(f"Successfully fetched coordinates: {property_info['Latitude']}, {property_info['Longitude']}")
            except Exception as e:
                log_print.warning(f"Failed to fetch coordinates: {e}")
        
        log_print.info(f"Testing with property: MLS {property_info['MLS Number']} - {property_info['Address']}")
        
        # Send Telegram notification (use default category for testing)
        telegram_message = format_telegram_message(property_info)
        send_telegram_message(
            message=telegram_message,
            parse_mode='HTML',
            caller_logger=log_print
        )
        
        log_print.info(f"Sent test Telegram notification for MLS {property_info['MLS Number']}")
        print(f"Test completed. Sent notification for existing property: MLS {property_info['MLS Number']}")
        return True
        
    except Exception as e:
        log_print.error(f"Error testing with existing property: {e}")
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--check-once':
            # Run single check
            log_print.info("Running single property check...")
            new_count = check_for_new_properties()
            
            if new_count == 0:
                # No new properties found, test with existing property
                test_with_existing_property()
            else:
                print(f"Property check completed. Found {new_count} new properties.")
        elif sys.argv[1] == '--continuous':
            # Run continuous monitoring
            run_continuous_monitor()
        else:
            print("Usage: python3 diane_all.py [--check-once|--continuous]")
    else:
        # Default: run single check
        log_print.info("Running single property check...")
        new_count = check_for_new_properties()
        
        if new_count == 0:
            # No new properties found, test with existing property
            test_with_existing_property()
        else:
            print(f"Property check completed. Found {new_count} new properties.")
