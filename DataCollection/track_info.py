'''
input: list of uris 
action: spotify api call to get track info
output: dictionary of track info
'''
import time
import os
import sys
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)
import DataCollection.spotify_access as SpotifyAccess

import pandas as pd
import spotipy
from spotipy import SpotifyException



def uris_from_csv(path):
    df = pd.read_csv(path)
    # make all uris are strings
    df['uri'] = df['uri'].astype(str)
    return df['uri'].tolist()

def consolidate_uris(files):
    consolidated_uris = set()
    for path in files:
        uris = uris_from_csv(path)
        consolidated_uris.update(uris)

    uri_list = []
    for uri in consolidated_uris:
        # Ensure valid URIs and filter out `nan` or improperly formatted strings
        if isinstance(uri, str) and uri.startswith("spotify:track:"):
            uri_list.append(uri.split(":")[-1])  # Extract the base62 ID
        elif pd.isnull(uri):  # Log and skip NaN values
            print(f"Skipping invalid URI: {uri}")
        else:
            uri_list.append(uri)  # Assume the URI is already in base62 format
    
    return uri_list


def results_to_csv(df, path):
    path = f"C:/Users/lnick/OneDrive/Desktop/Fall 2024/archives as data/final project/data/{path}"
    if df is not None:
        df.to_csv(path, index=False)


def songs_and_artists(sp, uris):
    start_time = time.time()
    print("starting clock")
    song_details = []
    artist_ids = []
    print(len(uris))

    for i in range(0, len(uris), 50):
        elapsed = time.time() - start_time
        print(f"time elapsed: {elapsed}")
        if elapsed > 300:
            print("time limit reached")
            break

        batch_ids = uris[i:i + 50]

        max_retries = 3
        for attempt in range(max_retries):
            try:
                print("try getting tracks")
                tracks = sp.tracks(batch_ids)
                print(f"Number of tracks returned: {len(tracks.get('tracks', []))}")
                break
            except SpotifyException as e:
                if e.http_status == 429:
                    # Rate limited: extract the wait time
                    retry_after = int(e.headers.get('Retry-After', 1))
                    print(f"Rate limited. Retrying after {retry_after} seconds...")
                    time.sleep(retry_after)
                    # After waiting, retry this request
                    continue
                else:
                    # Some other SpotifyException: log and skip this batch
                    print(f"SpotifyException encountered: {e}")
                    break
            except Exception as e:
                # Non-SpotifyException errors
                print(f"Encountered an error: {e}")
                break
        else:
            # If max_retries is reached and the loop did not break, skip this batch
            print("Max retries reached for this batch. Skipping.")
            continue

        # Process returned tracks
        print("processing tracks")
        for track in tracks.get("tracks", []):
            if track and "name" in track and "artists" in track and track["artists"]:
                
                name = track["name"]
                popularity = track.get("popularity", None)
                release_date = track["album"].get("release_date", None)
                artist_id = track["artists"][0].get("id", None)
                
                song_details.append({
                    "name": name,
                    "popularity": popularity,
                    "uri": track["uri"].split(":")[-1],
                    "release_date": release_date,
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
    print("getting song details")
    song_details, artist_ids = songs_and_artists(sp, uris)
    print("getting artist genres")
    artist_genres = get_artist_genres(sp, artist_ids)
    return song_details, artist_genres
    
def query(sp, file_paths):
    print("getting uris")
    uris = consolidate_uris(file_paths)
    #print("uris:", uris)
    
    song_details, artist_genres = api_calls(sp, uris)

    print("matching songs to genres")
    song_details = song_to_genre(song_details, artist_genres)
    details_df = pd.DataFrame(song_details)
    if 'artist_id' in details_df.columns:
        details_df = details_df.drop(columns=["artist_id"])
    
    print("writing to csv")
    results_to_csv(details_df, "track_info.csv")


if __name__ == "__main__":

    sp = SpotifyAccess.get_spotify_client()
    file_paths = [
        #"data/charts.csv",
        "data/user_listening.csv"
    ]

    query(sp, file_paths)