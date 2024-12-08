import requests

NAME = 'ArchivesClassProject'
API_KEY = '0a37afd3b0c40353b86d15aebe22c387'
SHARED_SECRET = 'a889b295667e91abe19231073f86ca32'
URL = 'http://ws.audioscrobbler.com/2.0/'


def country_top_tracks(country):
    params = {
        'method': 'geo.getTopTracks',
        'country': country,
        'api_key': API_KEY,
        'format': 'json'
    }
    response = requests.get(URL, params=params)
    data = response.json()
    return[
        {
            "track": track["name"],
            "artist": track["artist"]["name"]
        }
        for track in data["tracks"]["track"]
    ]

def country_top_artists(country):
    params = {
        'method': 'geo.getTopArtists',
        'country': country,
        'api_key': API_KEY,
        'format': 'json'
    }
    response = requests.get(URL, params=params)
    data = response.json()
    return[
        artist["name"]
        for artist in data["topartists"]["artist"]
    ]