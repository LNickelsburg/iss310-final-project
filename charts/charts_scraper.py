import requests
import pandas as pd
from datetime import datetime, timedelta


URL = "https://charts-spotify-com-service.spotify.com/auth/v0/charts/{query_type}-{region}-weekly/{date}"
HEADERS = {
    "accept": "application/json",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    "app-platform": "Browser",
    "authorization": "",
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



### PARAMS ###

def set_dates(n):
    today = datetime.now()
    last_thursday = today - timedelta(days=(today.weekday() - 3) % 7)
    dates = [(last_thursday - timedelta(weeks=i)).strftime("%Y-%m-%d") for i in range(n)]
    return dates

def set_url(query_type, region, date):
    return URL.format(query_type=query_type, region=region, date=date)

def set_headers(authorization):
    headers = HEADERS.copy()
    headers["authorization"] = authorization
    return headers


### HELPERS ###

def request_charts(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json() 
        entries = data.get("entries", [])  # Adjust key based on the API's JSON response
        df = pd.DataFrame(entries)
        return df
    else: return None

def clean_results(df):
    chart_entry_data = pd.json_normalize(df['chartEntryData'])
    track_metadata = pd.json_normalize(df['trackMetadata'])
    flattened = pd.concat([df.drop(['chartEntryData', 'missingRequiredFields', 'trackMetadata'], axis=1), chart_entry_data, track_metadata], axis=1)
    clean_results = flattened[['currentRank', 'trackName', 'trackUri', 'artists', 'releaseDate']]
    return clean_results

def results_to_csv(df, path):
    path = f"C:/Users/lnick/OneDrive/Desktop/Fall 2024/archives as data/final project/charts/{path}"
    if df is not None:
        df.to_csv(path, index=False)


### QUERY ###

def query(authorization, num_weeks, world, country, city):

    headers = set_headers(authorization)
    dates = set_dates(num_weeks)

    charts_data = {
        "date": [],
        "region": [],
        "details": []
    }

    for date in dates:
        print(f"Querying data for {date}...")
        if world:
            url = set_url('regional', 'global', date)
            charts = request_charts(url, headers)
            if charts is not None:
                results = clean_results(charts)
            else:
                results = None
            charts_data["date"].append(date)
            charts_data["region"].append("global")
            charts_data["details"].append(results)
        if country:
            print("Querying country data...")
            for country in COUNTRIES:
                url = set_url('regional', COUNTRIES[country], date)
                charts = request_charts(url, headers)
                if charts is not None:
                    results = clean_results(charts)
                else:
                    results = None
                charts_data["date"].append(date)
                charts_data["region"].append(country)
                charts_data["details"].append(results)
        if city:
            print("Querying city data...")
            for state in CITIES:    
                for city in CITIES[state]:
                    url = set_url('citytoptrack', city, date)
                    charts = request_charts(url, headers)
                    if charts is not None:
                        results = clean_results(charts)
                    else:
                        results = None
                    charts_data["date"].append(date)
                    charts_data["region"].append(city)
                    charts_data["details"].append(results)
    print("Query complete.")
    return pd.DataFrame(charts_data)



if __name__ == '__main__':

    authorization = "Bearer BQA687QiS7rhtRJUwWvqSFtWSgDDU4zBYf-g1pDExmLb4nOlhoFCIQoVsXVkzsJTkE3w87O3aYE834PAYDZDBYkXo_36LxSwq2FNf8gsNrzHdGHzytH61fsxSnQxaLNNob-eCYfknS0U9ypIvXZ1U20xHkmFNG3WvxHmzYmTiKwocb1kxTOWqYIupKUzLHFbjtX3bH6I"
    num_weeks = 2
    world = True
    country = True
    city = True

    results = query(authorization, num_weeks, world, country, city)
    results_to_csv(results, "charts_data.csv")
    