import requests
import pandas as pd
from datetime import datetime, timedelta


URL = "https://charts-spotify-com-service.spotify.com/auth/v0/charts/{query_type}-{region}-weekly/{date}"
HEADERS = {
    "accept": "application/json",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    "app-platform": "Browser",
    "authorization": "Bearer BQC7poeFpmHwVMD16yml40xaE4GwMmVJm6Wl7EdIkkYka9WaNmH2BCaGsioY75rE4s04RUUKGDU7C933ahc2A7O9TLdO2Qdugr2EAjAWMFGvB0H6WDUs5sFd-bwpjcLiae29DYVqyRI0t3NLloX_Lkn3mxUEIAR8jWcHy-Fr9aUsmJtE8WvfwpdvxZMh1eVGst6Zi0BR",  # Replace with a valid token
    "content-type": "application/json",
    "origin": "https://charts.spotify.com",
    "priority": "u=1, i",
    "referer": "https://charts.spotify.com/",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "spotify-app-version": "0.0.0.production",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}
QUERY_TYPES = ["regional", "citytoptrack"]
COUNTRIES = {
    "Global": "GLOBAL",
    "Argentina": "AR",
    "Australia": "AU",
    "Austria": "AT",
    "Belarus": "BY",
    "Belgium": "BE",
    "Bolivia": "BO",
    "Brazil": "BR",
    "Bulgaria": "BG",
    "Canada": "CA",
    "Chile": "CL",
    "Colombia": "CO",
    "Costa Rica": "CR",
    "Cyprus": "CY",
    "Czech Republic": "CZ",
    "Denmark": "DK",
    "Dominican Republic": "DO",
    "Ecuador": "EC",
    "Egypt": "EG",
    "El Salvador": "SV",
    "Estonia": "EE",
    "Finland": "FI",
    "France": "FR",
    "Germany": "DE",
    "Greece": "GR",
    "Guatemala": "GT",
    "Honduras": "HN",
    "Hong Kong": "HK",
    "Hungary": "HU",
    "Iceland": "IS",
    "India": "IN",
    "Indonesia": "ID",
    "Ireland": "IE",
    "Israel": "IL",
    "Italy": "IT",
    "Japan": "JP",
    "Kazakhstan": "KZ",
    "Latvia": "LV",
    "Lithuania": "LT",
    "Luxembourg": "LU",
    "Malaysia": "MY",
    "Mexico": "MX",
    "Morocco": "MA",
    "Netherlands": "NL",
    "New Zealand": "NZ",
    "Nicaragua": "NI",
    "Nigeria": "NG",
    "Norway": "NO",
    "Pakistan": "PK",
    "Panama": "PA",
    "Paraguay": "PY",
    "Peru": "PE",
    "Philippines": "PH",
    "Poland": "PL",
    "Portugal": "PT",
    "Romania": "RO",
    "Saudi Arabia": "SA",
    "Singapore": "SG",
    "Slovakia": "SK",
    "South Africa": "ZA",
    "South Korea": "KR",
    "Spain": "ES",
    "Sweden": "SE",
    "Switzerland": "CH",
    "Taiwan": "TW",
    "Thailand": "TH",
    "Turkey": "TR",
    "UAE": "AE",
    "Ukraine": "UA",
    "United Kingdom": "GB",
    "Uruguay": "UY",
    "USA": "US",
    "Venezuela": "VE",
    "Vietnam": "VN"
}
CITIES = {
    "California": ["Anaheim", "Los Angeles", "Sacramento", "San Diego", "San Francisco"],
    "Georgia": ["Atlanta"],
    "Texas": ["Austin", "Dallas", "Houston", "San Antonio"],
    "North Carolina": ["Charlotte"],
    "Illinois": ["Chicago"],
    "Ohio": ["Cleveland"],
    "Colorado": ["Denver"],
    "Michigan": ["Detroit"],
    "Indiana": ["Indianapolis"],
    "Nevada": ["Las Vegas"],
    "Tennessee": ["Memphis", "Nashville"],
    "Florida": ["Miami", "Tampa"],
    "Minnesota": ["Minneapolis"],
    "Louisiana": ["New Orleans"],
    "New York": ["New York City"],
    "Nebraska": ["Omaha"],
    "Pennsylvania": ["Philadelphia", "Pittsburgh"],
    "Oregon": ["Portland"],
    "Utah": ["Salt Lake City"],
    "Puerto Rico": ["San Juan"],
    "Washington": ["Seattle"],
    "Missouri": ["St Louis"],
    "District of Columbia": ["Washington"]
}


def generate_dates(n):
    today = datetime.now()
    last_thursday = today - timedelta(days=(today.weekday() - 3) % 7)
    dates = [(last_thursday - timedelta(weeks=i)).strftime("%Y-%m-%d") for i in range(n)]
    return dates

def set_url(query_type, region, date):
    return URL.format(query_type=query_type, region=region, date=date)

def get_charts(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json() 
        entries = data.get("entries", [])  # Adjust key based on the API's JSON response
        df = pd.DataFrame(entries)
        return df
    else: return None

def chart_to_csv(df, path):
    print("to csv")
    path = f"charts/{path}"
    if df is not None:
        df.to_csv(path, index=False)

def set_path(query_type, region, date):
    return f"{query_type}_{region}_{date}.csv"

def query(num_weeks, world, country, city):

    if num_weeks < 1:
        raise ValueError("The number of weeks must be at least 1.")
    if not (world or country or city):
        raise ValueError("At least one of 'world', 'country', or 'city' must be True.")
    
    dates = generate_dates(num_weeks)
    
    for date in dates:
        print(f"Querying for {date}...")
        if world:
            url = set_url('regional', 'global', date)
            results = get_charts(url, HEADERS)
            path = set_path('world', 'global', date)
            print(f"chart to csv for global at {path}")
            chart_to_csv(results, path)
        if country:
            for country in COUNTRIES:
                url = set_url('regional', COUNTRIES[country], date)
                results = get_charts(url, HEADERS)
                path = set_path('country', country, date)
                chart_to_csv(results, path)
        if city:
            for state in CITIES:
                for city in CITIES[state]:
                    url = set_url('citytoptrack', city, date)
                    results = get_charts(url, HEADERS)
                    path = set_path('city', city, date)
                    chart_to_csv(results, path)


if __name__ == '__main__':

    ### set parameters ###
    num_weeks = 1   
    world = True
    country = False
    city = False
    ######################

    query(num_weeks, world, country, city)
