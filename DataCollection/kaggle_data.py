import kagglehub
import shutil
import os
import pandas as pd


def download_dataset():
    path = kagglehub.dataset_download("rodolfofigueroa/spotify-12m-songs")
    filename = os.listdir(path)[0]
    path = os.path.join(path, filename)
    shutil.move(path, "data")

def clean_data():
    file = "DataCollection/data/tracks_features.csv"
    df = pd.read_csv(file)
    #drop columns
    df = df.drop(columns=['name', 'album', 'album_id', 'artists', 'artist_ids',
       'track_number', 'disc_number', 'explicit', 'duration_ms',
       'year', 'release_date'])
    #drop duplicates
    df = df.drop_duplicates()
    #drop null values
    df = df.dropna()
    #save to csv
    df.to_csv("DataCollection/data/tracks_features.csv", index=False)

def query():
    download_dataset()
    clean_data()