import pandas as pd
import os
import sys
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)
import spotify_access as SpotifyAccess



### API Calls ###

def recently_played(sp, limit):
    recently_played = sp.current_user_recently_played(limit=limit)
    tracks = recently_played['items']
    return tracks

def top_tracks(sp, time_range, limit):
    top_tracks = sp.current_user_top_tracks(time_range=time_range, limit=limit)
    tracks = top_tracks['items']
    return tracks


### HELPERS ###

def results_to_csv(df, path):
    path = f"C:/Users/lnick/OneDrive/Desktop/Fall 2024/archives as data/final project/DataCollection/{path}"
    if df is not None:
        df.to_csv(path, index=False)


### QUERY ###

def query(sp, limit, long_term, medium_term, short_term, recent):
    user_listening = {
        "category": [],
        "rank": [],
        "uri": []
    }


    print(f"Querying data...")
    if long_term:
        tracks = top_tracks(sp, 'long_term', limit)
        for i, track in enumerate(tracks):
            user_listening["category"].append("long_term")
            user_listening["rank"].append(i+1)
            user_listening["uri"].append(track['uri'].split(":")[-1])
    if medium_term:
        tracks = top_tracks(sp, 'medium_term', limit)
        for i, track in enumerate(tracks):
            user_listening["category"].append("medium_term")
            user_listening["rank"].append(i+1)
            user_listening["uri"].append(track['uri'].split(":")[-1])
    if short_term:
        tracks = top_tracks(sp, 'short_term', limit)
        for i, track in enumerate(tracks):
            user_listening["category"].append("short_term")
            user_listening["rank"].append(i+1)
            user_listening["uri"].append(track['uri'].split(":")[-1])
    if recent:
        tracks = recently_played(sp, limit)
        for i, track in enumerate(tracks):
            user_listening["category"].append("recent")
            user_listening["rank"].append(i+1)
            user_listening["uri"].append(track['track']['uri'].split(":")[-1])




    print("Query complete.")
    output = pd.DataFrame(user_listening)
    results_to_csv(output, "data/user_listening.csv")



if __name__ == "__main__":

    scope = "user-read-recently-played user-top-read"
    limit = 3
    long_term = True
    medium_term = False
    short_term = False
    recent = True

    sp = SpotifyAccess.get_spotify_client(scope)
    query(sp, limit, long_term, medium_term, short_term, recent)

    



    



