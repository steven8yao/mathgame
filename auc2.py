import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os
import json
import math
from urllib.parse import quote_plus
from config import google_geocode_api_key
from ib_tg import send_telegram_message
from datetime import datetime, timedelta

# Reference points
REFS = {
    "calgary": {"lat": 51.060428, "lng": -114.180062},
    "edmonton": {"lat": 53.588009, "lng": -113.431407}
}

COMBINED_CACHE_FILE = "auction_location_cache.json"
BASE_URL = "https://www.liveauctionworld.com"
AUCTION_LIST_URL = f"{BASE_URL}/auctionlist.aspx?ps=100"
EXCEL_FILE = "auction.xlsx"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

geo_HEADERS = {
    'User-Agent': 'auction-test/1.0 (steven8.yao@gmail.com)',
    'Referer': 'https://gisinv.com/',
    'From': 'steven8.yao@gmail.com'
}


def load_combined_cache(filename):
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_combined_cache(filename, cache):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def geocode_address(address, geocode_cache):
    # Ignore ambiguous addresses like 'Address Coming'
    ambiguous_words = ["address coming", "tba", "to be announced", "coming soon", "unknown", "n/a", "not available"]
    def strip_ambiguous_prefix(addr):
        addr_strip = addr.strip()
        for word in ambiguous_words:
            if addr_strip.lower().startswith(word):
                # Remove the ambiguous word and any following comma/space
                rest = addr_strip[len(word):].lstrip(", ")
                return rest
        return addr_strip

    # Always use the full address as the cache key
    cache_key = address or ""
    checked_address = cache_key.strip()
    cleaned_address = strip_ambiguous_prefix(checked_address)
    # If after stripping, it's empty or still ambiguous, skip
    if not cleaned_address or any(word in cleaned_address.lower() for word in ambiguous_words):
        if cache_key not in geocode_cache:
            geocode_cache[cache_key] = {}
        geocode_cache[cache_key]["geocode"] = [None, None]
        return None, None
    # Use the full address as the cache key, but geocode with the cleaned address
    address_to_geocode = cleaned_address
    address = cache_key

    # Use Nominatim (OpenStreetMap) for geocoding
    if address in geocode_cache and "geocode" in geocode_cache[address]:
        cached = geocode_cache[address]["geocode"]
        if cached is not None and cached != [None, None]:
            return tuple(cached)
        # else, fall through to recalculate
    url = f"https://nominatim.openstreetmap.org/search?q={quote_plus(address_to_geocode)}&format=json&limit=1"
    try:
        resp = requests.get(url, headers={"User-Agent": geo_HEADERS["User-Agent"]}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data:
            lat, lng = float(data[0]["lat"]), float(data[0]["lon"])
            if address not in geocode_cache:
                geocode_cache[address] = {}
            geocode_cache[address]["geocode"] = [lat, lng]
            return lat, lng
    except Exception:
        pass

    # If failed, try removing ordinal suffixes (st, nd, rd, th) from street numbers
    import re
    def remove_ordinal_suffixes(addr):
        # Replace patterns like '227th', '21st', '22nd', '23rd' with just the number
        return re.sub(r'(\b\d+)(st|nd|rd|th)(\b)', r'\1', addr, flags=re.IGNORECASE)

    simplified_address = remove_ordinal_suffixes(address_to_geocode)
    if simplified_address != address_to_geocode:
        url2 = f"https://nominatim.openstreetmap.org/search?q={quote_plus(simplified_address)}&format=json&limit=1"
        try:
            resp2 = requests.get(url2, headers={"User-Agent": geo_HEADERS["User-Agent"]}, timeout=15)
            resp2.raise_for_status()
            data2 = resp2.json()
            if data2:
                lat, lng = float(data2[0]["lat"]), float(data2[0]["lon"])
                if address not in geocode_cache:
                    geocode_cache[address] = {}
                geocode_cache[address]["geocode"] = [lat, lng]
                return lat, lng
        except Exception:
            pass

    # If still failed, try Google Geocoding API
    try:
        google_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={quote_plus(address_to_geocode)}&key={google_geocode_api_key}"
        resp3 = requests.get(google_url, timeout=15)
        resp3.raise_for_status()
        data3 = resp3.json()
        if data3.get("status") == "OK" and data3.get("results"):
            loc = data3["results"][0]["geometry"]["location"]
            lat, lng = loc["lat"], loc["lng"]
            if address not in geocode_cache:
                geocode_cache[address] = {}
            geocode_cache[address]["geocode"] = [lat, lng]
            return lat, lng
    except Exception:
        pass

    if address not in geocode_cache:
        geocode_cache[address] = {}
    geocode_cache[address]["geocode"] = [None, None]
    return None, None

def calculate_driving_time_osrm(origin_lat, origin_lng, dest_lat, dest_lng, route_cache, cache_key):
    # Use OSRM public API, cache by address and city name (Calgary/Edmonton)
    address, city = cache_key.rsplit("|", 1)
    city = city.strip().capitalize()  # Ensure 'Calgary'/'Edmonton' capitalization
    if address in route_cache and "route" in route_cache[address] and city in route_cache[address]["route"]:
        cached = route_cache[address]["route"][city]
        if cached is not None and cached != [None, None, None]:
            return tuple(cached)
        # else, fall through to recalculate
    base_url = "http://router.project-osrm.org/route/v1/driving"
    url = f"{base_url}/{origin_lng},{origin_lat};{dest_lng},{dest_lat}?overview=false&alternatives=false&steps=false"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data['code'] == 'Ok' and data['routes']:
            route = data['routes'][0]
            duration_minutes = int(route['duration'] / 60)
            distance_km = route['distance'] / 1000
            direction = calculate_direction(origin_lat, origin_lng, dest_lat, dest_lng)
            if address not in route_cache:
                route_cache[address] = {}
            if "route" not in route_cache[address]:
                route_cache[address]["route"] = {}
            route_cache[address]["route"][city] = [duration_minutes, distance_km, direction]
            return duration_minutes, distance_km, direction
    except Exception:
        pass
    if address not in route_cache:
        route_cache[address] = {}
    if "route" not in route_cache[address]:
        route_cache[address]["route"] = {}
    route_cache[address]["route"][city] = [None, None, None]
    return None, None, None

def calculate_direction(origin_lat, origin_lng, dest_lat, dest_lng):
    # Returns compass direction (N, NE, E, etc.)
    lat1 = math.radians(origin_lat)
    lat2 = math.radians(dest_lat)
    delta_lng = math.radians(dest_lng - origin_lng)
    y = math.sin(delta_lng) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(delta_lng)
    bearing_rad = math.atan2(y, x)
    bearing_deg = (math.degrees(bearing_rad) + 360) % 360
    dirs = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
    ix = int((bearing_deg + 22.5) // 45) % 8
    return dirs[ix]

def get_auction_links():
    # Fetch and parse the live auction list from AUCTION_LIST_URL
    response = requests.get(AUCTION_LIST_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()
    html = response.text
    soup = BeautifulSoup(html, "html.parser")
    auction_list = []
    for row in soup.select("div.auctionslisting > div.row"):
        auction = {}
        # Auction URL
        a_thumb = row.find("a", class_="row_thumbnail")
        auction_url = a_thumb["href"] if a_thumb and a_thumb.get("href") else ""
        auction["auction_url"] = auction_url
        # Auction Title
        a_title = row.find("span", class_="title")
        auction["auction_title"] = a_title.get_text(strip=True) if a_title else ""
        # Auction Image
        auction["auction_image"] = a_thumb.find("img")["src"] if a_thumb and a_thumb.find("img") and a_thumb.find("img").get("src") else ""
        # Auctioneer
        auctioneer_div = row.find("div", class_="auctioneer")
        auction["auctioneer"] = auctioneer_div.get_text(strip=True) if auctioneer_div else ""
        # Location
        location_div = row.find("div", class_="location")
        auction["auction_location"] = location_div.get_text(strip=True) if location_div else ""
        # Date/time
        datetime_div = row.find("div", class_="datetime")
        auction["auction_datetime"] = datetime_div.get_text(strip=True) if datetime_div else ""
        # Number of lots (from .linkinfo)
        linkinfo = row.find("span", class_="linkinfo")
        auction["auction_lots"] = linkinfo.get_text(strip=True) if linkinfo else ""
        # Status: treat as 'Complete' if links_ul contains 'Bidding Has Concluded', otherwise keep
        links_ul = row.find("ul", class_="links")
        is_complete = False
        if links_ul and 'Bidding Has Concluded' in links_ul.get_text():
            is_complete = True
        # print(auction["auction_title"], auction["auction_location"], is_complete)
        # Only keep auctions with location including 'Alberta' and not complete
        if (
            "alberta" in auction["auction_location"].lower()
            and not is_complete
        ):
            auction_list.append(auction)
    return auction_list

def get_auction_pages_ps100(auction_url):
    """Return all auction page URLs with ps=100 (handle pagination)."""
    if "?" in auction_url:
        base_url = auction_url.split("?")[0]
    else:
        base_url = auction_url
    pages = []
    page_num = 1
    while True:
        if page_num == 1:
            url = f"{base_url}?ps=100"
        else:
            url = f"{base_url}_p{page_num}?ps=100"
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            break
        soup = BeautifulSoup(resp.content, "html.parser")
        items = soup.select("div.gridItem")
        if not items:
            break
        pages.append(url)
        if len(items) < 100:
            break
        page_num += 1
    return pages

def get_items_from_auction_page(page_url):
    """Extract all item links and basic info from an auction page."""
    response = requests.get(page_url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    items = []
    for grid_item in soup.select("div.gridItem"):
        # Item URL and Title
        a_thumb = grid_item.find("a", class_="row_thumbnail")
        a_title = grid_item.find("span", class_="gridView_heading")
        if a_title:
            a_title = a_title.find("a")
        # Prefer the title from gridView_title if available
        title = ""
        if a_title:
            title_span = a_title.find("span", class_="gridView_title")
            if title_span:
                title = title_span.get_text(strip=True)
            else:
                title = a_title.get("title", "")
        # Image
        img = a_thumb.find("img") if a_thumb else None
        image_url = img["src"] if img and img.get("src") else ""
        # Description
        desc_div = grid_item.find("div", class_="description gridView_description")
        description = desc_div.get_text(strip=True) if desc_div else ""
        # Estimate
        estimate_div = grid_item.find("div", class_="startpriceestimates")
        estimate = estimate_div.get_text(strip=True) if estimate_div else ""
        # Winning bid
        winning_bid_div = grid_item.find("div", class_="gridView_winningbid")
        winning_bid = winning_bid_div.get_text(strip=True) if winning_bid_div else ""
        # Item details URL
        item_url = ""
        if a_title and a_title.get("href"):
            item_url = a_title["href"]
            if item_url.startswith("/"):
                item_url = BASE_URL + item_url
        elif a_thumb and a_thumb.get("href"):
            item_url = a_thumb["href"]
            if item_url.startswith("/"):
                item_url = BASE_URL + item_url
        if item_url:
            items.append({
                "item_url": item_url,
                "title": title,
                "image_url": image_url,
                "description": description,
                "estimate": estimate,
                "winning_bid": winning_bid
            })
    return items

def get_item_details_full(item_url):
    """Extract all available details from an individual item page."""
    response = requests.get(item_url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    details = {}

    # --- Live Auction Additions ---
    # 1. Bidding Ends At (from countdown_datetime)
    countdown_dt = soup.find("span", id="countdown_datetime")
    details["bidding_ends_at"] = countdown_dt.get_text(strip=True) if countdown_dt else ""

    # 2. Lot Association Info (synchronized deadlines, extension rules)
    lot_assoc = soup.find("span", id="cphBody_lblLotAssociationsInfo")
    details["lot_association_info"] = lot_assoc.get_text(strip=True) if lot_assoc else ""

    # 3. Other Details (buyer fee, preview, payment, removal, etc.)
    other_details = soup.find("span", id="cphBody_litOtherDetails")
    details["other_details"] = other_details.get_text(" ", strip=True) if other_details else ""

    # --- End Live Auction Additions ---

    # Auctioneer Name (breadcrumbs and side panel)
    auctioneer = ""
    auctioneer_tag = soup.find("a", id="cphBody_hlBreadcrumb_AuctioneerName")
    if auctioneer_tag:
        auctioneer = auctioneer_tag.get_text(strip=True)
    else:
        # fallback: side panel
        auctioneer_side = soup.find("a", id="cphBody_ucAuctioneerSidePanel_imgAuctioneerLogo")
        if auctioneer_side and auctioneer_side.get("title"):
            auctioneer = auctioneer_side["title"]
    details["auctioneer"] = auctioneer

    # Auction Name (breadcrumbs)
    auction_name = ""
    auction_name_tag = soup.find("a", id="cphBody_hlBreadcrumb_AuctionTitle")
    if auction_name_tag:
        auction_name = auction_name_tag.get_text(strip=True)
    details["auction_name"] = auction_name

    # # Item Title (breadcrumbs and h1)
    # item_title = ""
    # h1_tag = soup.find("h1", itemprop="name")
    # if h1_tag:
    #     item_title = h1_tag.get_text(strip=True)
    # details["item_title"] = item_title

    # Lot Number (in <i> inside .pageheading)
    lot_number = ""
    pageheading = soup.find("div", class_="pageheading")
    if pageheading:
        i_tag = pageheading.find("i")
        if i_tag:
            lot_number = i_tag.get_text(strip=True)
    details["lot_number"] = lot_number

    # Category (in .pageheadingsub)
    category = ""
    pageheadingsub = soup.find("div", class_="pageheadingsub")
    if pageheadingsub:
        cat_tag = pageheadingsub.find("a", id="cphBody_hlSubBar_ItemCategory")
        if cat_tag:
            category = cat_tag.get_text(strip=True)
    details["category"] = category

    # Currency (in .pageheadingsub)
    currency = ""
    if pageheadingsub:
        cur_tag = pageheadingsub.find("a", id="cphBody_hlSubBar_AuctionCurrency")
        if cur_tag:
            currency = cur_tag.get_text(strip=True)
    details["currency"] = currency

    # Start Price (in .pageheadingsub)
    start_price = ""
    if pageheadingsub:
        for span in pageheadingsub.find_all("span", class_="part"):
            if "Start Price:" in span.get_text():
                start_price = span.get_text(strip=True).replace("Start Price:", "").strip()
    details["start_price"] = start_price

    # Estimate (in .pageheadingsub)
    estimate = ""
    est_span = pageheadingsub.find("span", id="cphBody_spanEstimates") if pageheadingsub else None
    if est_span:
        estimate = est_span.get_text(strip=True).replace("Estimated At:", "").strip()
    details["estimate"] = estimate

    # # Main Image URL (in #item_media_main img)
    # main_image_url = ""
    # img_tag = soup.select_one("#item_media_main img")
    # if img_tag and img_tag.get("src"):
    #     main_image_url = img_tag["src"]
    # details["main_image_url"] = main_image_url

    # # Main Image Full URL (in #item_media_main a)
    # main_image_full_url = ""
    # a_img = soup.select_one("#item_media_main a")
    # if a_img and a_img.get("href"):
    #     main_image_full_url = a_img["href"]
    # details["main_image_full_url"] = main_image_full_url

    # Current Bid (in #item_bidding_currentbid .amount)
    current_bid = ""
    bid_tag = soup.select_one("#item_bidding_currentbid .amount")
    if bid_tag:
        current_bid = bid_tag.get_text(strip=True)
    details["current_bid"] = current_bid

    # Current Bid Currency (in #item_bidding_currentbid .currency)
    current_bid_currency = ""
    bid_cur_tag = soup.select_one("#item_bidding_currentbid .currency")
    if bid_cur_tag:
        current_bid_currency = bid_cur_tag.get_text(strip=True)
    details["current_bid_currency"] = current_bid_currency

    # Current Bidder (in #item_bidding_currentbid .username)
    current_bidder = ""
    bid_user_tag = soup.select_one("#item_bidding_currentbid .username")
    if bid_user_tag:
        current_bidder = bid_user_tag.get_text(strip=True)
    details["current_bidder"] = current_bidder

    # Fees Text (in #item_bidding_currentbid .fees)
    fees_text = ""
    fees_tag = soup.select_one("#item_bidding_currentbid .fees")
    if fees_tag:
        fees_text = fees_tag.get_text(strip=True)
    details["fees_text"] = fees_text

    # Lot Description (in #item_details_info_description)
    lot_description = ""
    desc_tag = soup.find("span", id="item_details_info_description")
    if desc_tag:
        lot_description = desc_tag.get_text(strip=True)
    details["lot_description"] = lot_description

    # Location (in #item_details_info_preview)
    location = ""
    preview_tag = soup.find("span", id="item_details_info_preview")
    if preview_tag:
        loc_b = preview_tag.find("b")
        if loc_b and "Auction Location:" in loc_b.get_text():
            loc_b.extract()  # Remove the <b> tag
        location = preview_tag.get_text(separator=" ", strip=True)
        # Remove previewing details if present
        if "Previewing Details:" in location:
            location = location.split("Previewing Details:")[0].strip()
    details["location"] = location

    # Previewing Details (in #item_details_info_preview)
    previewing_details = ""
    if preview_tag:
        preview_info = preview_tag.find("span", id="cphBody_cbItemPreviewInfo")
        if preview_info:
            previewing_details = preview_info.get_text(strip=True)
    details["previewing_details"] = previewing_details

    # Taxes, Buyer's Premiums, and Additional Fees (in #item_details_info_taxes)
    taxes_tag = soup.find("span", id="item_details_info_taxes")
    # Taxes
    taxes = []
    if taxes_tag:
        # Look for a table with headers Tax, Rate, Desc.
        for table in taxes_tag.find_all("table", class_="datainfo"):
            thead = table.find("thead")
            if thead:
                headers = [th.get_text(strip=True).lower() for th in thead.find_all("td")]
                if headers == ["tax", "rate", "desc."]:
                    for row in table.find_all("tr")[1:]:
                        cols = [td.get_text(strip=True) for td in row.find_all("td")]
                        if cols:
                            taxes.append(" | ".join(cols))
    details["taxes"] = "; ".join(taxes)

    # Buyer's Premiums
    buyers_premiums = []
    if taxes_tag:
        for table in taxes_tag.find_all("table", class_="datainfo"):
            thead = table.find("thead")
            if thead:
                headers = [th.get_text(strip=True).lower() for th in thead.find_all("td")]
                if headers == ["from (incl.)", "to (excl.)", "premium"] or headers == ["from (inc.)", "to (exc.)", "premium"]:
                    for row in table.find_all("tr")[1:]:
                        cols = [td.get_text(strip=True) for td in row.find_all("td")]
                        if cols:
                            buyers_premiums.append(" | ".join(cols))
    details["buyers_premiums"] = "; ".join(buyers_premiums)

    # Additional Fees
    additional_fees = ""
    if taxes_tag:
        fees_table = taxes_tag.find("table", id="cphBody_gvFees")
        if fees_table:
            additional_fees = fees_table.get_text(separator=" ", strip=True)
    details["additional_fees"] = additional_fees

    # Shipping Details (in #item_details_info_shippay)
    shipping_details = ""
    shippay_tag = soup.find("span", id="item_details_info_shippay")
    if shippay_tag:
        ship_info = shippay_tag.find("span", id="cphBody_cbItemShippingInfo")
        if ship_info:
            shipping_details = ship_info.get_text(strip=True)
    details["shipping_details"] = shipping_details

    # Payment Details (in #item_details_info_shippay)
    payment_details = ""
    if shippay_tag:
        pay_info = shippay_tag.find("span", id="cphBody_cbItemPaymentInfo")
        if pay_info:
            payment_details = pay_info.get_text(strip=True)
    details["payment_details"] = payment_details

    # Accepted Payment Methods (in #item_details_info_shippay ul)
    accepted_payment_methods = []
    if shippay_tag:
        pay_ul = shippay_tag.find("ul")
        if pay_ul:
            for li in pay_ul.find_all("li"):
                accepted_payment_methods.append(li.get_text(strip=True))
    details["accepted_payment_methods"] = ", ".join(accepted_payment_methods)

    # Terms (in #item_details_info_terms)
    terms = ""
    terms_tag = soup.find("span", id="item_details_info_terms")
    if terms_tag:
        terms = terms_tag.get_text(separator=" ", strip=True)
    details["terms"] = terms

    # # Auctioneer Logo (in side panel)
    # auctioneer_logo = ""
    # logo_tag = soup.find("img", id="cphBody_ucAuctioneerSidePanel_imgAuctioneerLogo")
    # if logo_tag and logo_tag.get("src"):
    #     auctioneer_logo = logo_tag["src"]
    # details["auctioneer_logo"] = auctioneer_logo

    # Auctioneer Location (in side panel)
    auctioneer_location = ""
    auctioneer_loc_tag = None
    for span in soup.find_all("span"):
        if span.get("style") and "Camarillo" in span.get_text():
            auctioneer_loc_tag = span
            break
    if auctioneer_loc_tag:
        auctioneer_location = auctioneer_loc_tag.get_text(strip=True)
    details["auctioneer_location"] = auctioneer_location

    # # Auctioneer Phone (in side panel)
    # auctioneer_phone = ""
    # for span in soup.find_all("span"):
    #     if span.get("style") and "866-392-6229" in span.get_text():
    #         auctioneer_phone = span.get_text(strip=True)
    #         break
    # details["auctioneer_phone"] = auctioneer_phone

    return details

def run_auction_monitor():
    while True:
        auction_infos = get_auction_links()
        if not auction_infos:
            print("No Alberta auctions found.")
            time.sleep(600)
            continue
        combined_cache = load_combined_cache(COMBINED_CACHE_FILE)
        geocode_cache = combined_cache
        route_cache = combined_cache
        # Load existing Excel if exists
        if os.path.exists(EXCEL_FILE):
            df = pd.read_excel(EXCEL_FILE)
        else:
            df = pd.DataFrame()
        item_url_set = set(df["item_url"]) if not df.empty and "item_url" in df.columns else set()
        for auction_idx, auction_info in enumerate(auction_infos[:3]):
            auction_url = auction_info["auction_url"]
            print(f"Processing Alberta auction {auction_idx+1}: {auction_url}")
            auction_pages = get_auction_pages_ps100(auction_url)
            items = []
            for page_url in auction_pages:
                items.extend(get_items_from_auction_page(page_url))
            idx = 0
            while idx < len(items):
                item = items[idx]
                print(f"  Processing item {idx+1}: {item['title']}")
                details = get_item_details_full(item["item_url"])
                # Check if item is concluded
                if 'Bidding Has Concluded' in details.get('other_details', ''):
                    # Save to Excel but do not report
                    row = {**auction_info, **item, **details}
                    auction_address = row.get("location") or row.get("auction_location", "")
                    lat, lng = geocode_address(auction_address, geocode_cache)
                    row["auction_lat"] = lat
                    row["auction_lng"] = lng
                    for ref_name, ref in REFS.items():
                        if lat is not None and lng is not None:
                            cache_key = f"{auction_address}|{ref_name.capitalize()}"
                            duration, distance, direction = calculate_driving_time_osrm(ref["lat"], ref["lng"], lat, lng, route_cache, cache_key)
                        else:
                            duration, distance, direction = None, None, None
                        row[f"{ref_name}_drive_minutes"] = duration
                        row[f"{ref_name}_drive_km"] = distance
                        row[f"{ref_name}_direction"] = direction
                    # Reporting columns
                    row["reported within 12 hours"] = row.get("reported within 12 hours", "")
                    row["reported within 1 hour"] = row.get("reported within 1 hour", "")
                    if item["item_url"] in item_url_set:
                        df.loc[df["item_url"] == item["item_url"], list(row.keys())] = list(row.values())
                    else:
                        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
                        item_url_set.add(item["item_url"])
                    idx += 1
                    continue
                # Merge all fields from item, details, and auction info
                row = {**auction_info, **item, **details}
                auction_address = row.get("location") or row.get("auction_location", "")
                lat, lng = geocode_address(auction_address, geocode_cache)
                row["auction_lat"] = lat
                row["auction_lng"] = lng
                for ref_name, ref in REFS.items():
                    if lat is not None and lng is not None:
                        cache_key = f"{auction_address}|{ref_name.capitalize()}"
                        duration, distance, direction = calculate_driving_time_osrm(ref["lat"], ref["lng"], lat, lng, route_cache, cache_key)
                    else:
                        duration, distance, direction = None, None, None
                    row[f"{ref_name}_drive_minutes"] = duration
                    row[f"{ref_name}_drive_km"] = distance
                    row[f"{ref_name}_direction"] = direction
                # Save combined cache after each item
                save_combined_cache(COMBINED_CACHE_FILE, combined_cache)
                # Prepare Telegram message
                image_url = row.get("image_url", "")
                title = row.get("title", "")
                item_url = row.get("item_url", "")
                description = row.get("description", "")
                current_bid = row.get("current_bid", "")
                current_bid_currency = row.get("current_bid_currency", "")
                bidding_ends_at = row.get("bidding_ends_at", "")
                calgary_drive_minutes = row.get("calgary_drive_minutes", "")
                edmonton_drive_minutes = row.get("edmonton_drive_minutes", "")
                # Parse and calculate remaining time
                remaining_str = ""
                reported_12 = False
                reported_1 = False
                if "reported within 12 hours" in row:
                    reported_12 = bool(row["reported within 12 hours"])
                if "reported within 1 hour" in row:
                    reported_1 = bool(row["reported within 1 hour"])
                # Parse bidding_ends_at: e.g. '2025 Jul 16 @ 12:00(UTC-06:00 : CST/MDT)'
                try:
                    import re
                    match = re.search(r'(\d{4}) ([A-Za-z]{3}) (\d{1,2}) @ (\d{1,2}):(\d{2})', bidding_ends_at)
                    if match:
                        year, mon, day, hour, minute = match.groups()
                        month = time.strptime(mon, '%b').tm_mon
                        dt_end = datetime(int(year), int(month), int(day), int(hour), int(minute))
                        now = datetime.now()
                        remaining = dt_end - now
                        if remaining.total_seconds() > 0:
                            hours = remaining.days * 24 + remaining.seconds // 3600
                            minutes = (remaining.seconds % 3600) // 60
                            remaining_str = f"{hours}h {minutes}m"
                        else:
                            remaining_str = "Ended"
                    else:
                        remaining_str = "Unknown"
                except Exception:
                    remaining_str = "ParseError"
                # If next item is not urgent (>12h), sleep and continue
                if remaining_str != "Ended" and remaining_str != "Unknown" and remaining_str != "ParseError" and 'h' in remaining_str:
                    h = int(remaining_str.split('h')[0])
                    if h >= 48:
                        mins = random.randint(40, 80)
                        print(f"Next item more than 12 hours away, sleeping for {mins} minutes...")
                        df.to_excel(EXCEL_FILE, index=False)
                        time.sleep(mins * 60)
                        continue  # re-check this item after sleep
                # Telegram reporting logic
                should_report = False
                if remaining_str != "Ended" and remaining_str != "Unknown" and remaining_str != "ParseError":
                    # Only report if not already reported within 12h, and within 12h
                    if not reported_12 and 'h' in remaining_str:
                        h = int(remaining_str.split('h')[0])
                        if h < 48:
                            should_report = True
                            row["reported within 12 hours"] = True
                    # If already reported within 12h, but not within 1h, and now within 1h
                    if reported_12 and not reported_1 and 'h' in remaining_str:
                        h = int(remaining_str.split('h')[0])
                        if h < 1:
                            should_report = True
                            row["reported within 1 hour"] = True
                # Compose message
                msg = f'<a href="{item_url}">{title}</a>\n'
                # if image_url:
                #     msg += f'<a href="{image_url}">🖼️ Image</a>\n'
                # msg += f"<b>Description:</b> {description[:300]}\n"
                msg += f"<b>Current Bid:</b> {current_bid} {current_bid_currency}\n"
                msg += f"<b>Bidding Ends At:</b> {bidding_ends_at} ({remaining_str})\n"
                msg += f"<b>Calgary Drive (min):</b> {calgary_drive_minutes} | <b>Edmonton Drive (min):</b> {edmonton_drive_minutes}"
                if should_report:
                    try:
                        send_telegram_message(msg, parse_mode='HTML')
                        print(f"Reported: {title} - {remaining_str}")
                    except Exception as e:
                        print(f"Failed to send Telegram message: {e}")
                # Save/update row in DataFrame, handle dtype compatibility
                import numpy as np
                def clean_row_types(row_dict, df):
                    cleaned = row_dict.copy()
                    for col in df.columns:
                        if col in cleaned:
                            # If column is numeric and value is '', set to np.nan
                            if pd.api.types.is_numeric_dtype(df[col]) and cleaned[col] == '':
                                cleaned[col] = np.nan
                    return cleaned
                if item_url in item_url_set:
                    cleaned_row = clean_row_types(row, df)
                    for k, v in cleaned_row.items():
                        if k in df.columns:
                            df.loc[df["item_url"] == item_url, k] = v
                else:
                    # For new rows, ensure types match existing DataFrame
                    if not df.empty:
                        cleaned_row = clean_row_types(row, df)
                    else:
                        cleaned_row = row
                    df = pd.concat([df, pd.DataFrame([cleaned_row])], ignore_index=True)
                    item_url_set.add(item_url)
                time.sleep(random.uniform(5, 10))  # 5-10 seconds sleep
                idx += 1
        df.to_excel(EXCEL_FILE, index=False)
        print(f"Saved {len(df)} items to {EXCEL_FILE}")

if __name__ == "__main__":
    run_auction_monitor()