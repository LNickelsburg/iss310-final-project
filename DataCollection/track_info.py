'''
input: list of uris 
action: spotify api call to get track info
output: dictionary of track info
'''

import os
import sys
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)
import spotify_access as SpotifyAccess

import pandas as pd



def uris_from_csv(path):
    df = pd.read_csv(path)
    return df['uri'].tolist()

def consolidate_uris(files):
    consolidated_uris = set()
    for path in files:
        uris = uris_from_csv(path)
        consolidated_uris.update(uris)
    uri_list = list(consolidated_uris)
    return [uri.split(":")[-1] for uri in uri_list]


def results_to_csv(df, path):
    path = f"C:/Users/lnick/OneDrive/Desktop/Fall 2024/archives as data/final project/DataCollection/{path}"
    if df is not None:
        df.to_csv(path, index=False)


def songs_and_artists(sp, uris):
    song_details = []
    artist_ids = []
    for i in range(0, len(uris), 50):
        batch_ids = uris[i:i + 50]
        tracks = sp.tracks(batch_ids)
        for track in tracks["tracks"]:
            name = track["name"]
            release_date = track["album"]["release_date"]
            artist_id = track["artists"][0]["id"]
            song_details.append({
                "name": name,
                "uri": track["uri"].split(":")[-1],
                "date": release_date,
                "artist_id": artist_id
            })
            artist_ids.append(artist_id)
    return song_details, artist_ids

def get_artist_genres(sp, artist_ids):
    artist_genres = {}
    for i in range(0, len(artist_ids), 50):
        batch_ids = artist_ids[i:i + 50]
        artists = sp.artists(batch_ids)
        for artist in artists["artists"]:
            artist_genres[artist["id"]] = artist["genres"]
    return artist_genres

def song_to_genre(song_details, artist_genres):
    for song in song_details:
        artist_id = song["artist_id"]
        if artist_id in artist_genres:
            song["genre"] = artist_genres[artist_id]
    return song_details

def api_calls(sp, uris):
    song_details, artist_ids = songs_and_artists(sp, uris)
    artist_genres = get_artist_genres(sp, artist_ids)
    return song_details, artist_genres
    
def query(sp, file_paths):
    uris = consolidate_uris(file_paths)
    song_details, artist_genres = api_calls(sp, uris)

    song_details = song_to_genre(song_details, artist_genres)
    details_df = pd.DataFrame(song_details)
    details_df = details_df.drop(columns=["artist_id"])
    results_to_csv(details_df, "data/track_info.csv")


if __name__ == "__main__":

    sp = SpotifyAccess.get_spotify_client()

    file_paths = [
        "DataCollection/data/charts.csv",
        "DataCollection/data/user_listening.csv"
    ]

    query(sp, file_paths)





