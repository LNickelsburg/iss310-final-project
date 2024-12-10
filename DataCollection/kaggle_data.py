import kagglehub
import shutil
import os
import pandas as pd


def download_dataset(dataset):
    path = kagglehub.dataset_download(dataset)
    file_list = os.listdir(path)
    paths = []
    for file in file_list:
        if file.endswith(".csv"):
            file = os.path.join(path, file)
            paths.append(file)
    return paths

def clean_data(paths):
    for path in paths:
        df = pd.read_csv(path)

        # rename id and track_id to uri
        if "id" in df.columns:
            df.rename(columns={"id": "uri"}, inplace=True)
        if "track_id" in df.columns:
            df.rename(columns={"track_id": "uri"}, inplace=True)

        df = df[["uri", "danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "time_signature"]]
        df = df.drop_duplicates()
        df = df.dropna()

        save_data(df, path.split("\\")[-1])

def save_data(df, name):
    df.to_csv(f"DataCollection/data/audio_features/{name}", index=False)

def query(datasets):

    paths = []
    for dataset in datasets:
        paths.append(download_dataset(dataset))

    csvs = []
    for path in paths:
        for p in path:
            csvs.append(p)
    csvs = clean_data(csvs)

def combine_data():
    directory = "DataCollection/data/audio_features"
    files = [f for f in os.listdir(directory) if f.endswith(".csv")]
    combined_df = pd.concat([pd.read_csv(os.path.join(directory, f)) for f in files])
    combined_df = combined_df.drop_duplicates()
    combined_df.to_csv("DataCollection/data/modified/tracks_features.csv", index=False)

if __name__ == '__main__':
    datasets = ["rodolfofigueroa/spotify-12m-songs",
                "tomigelo/spotify-audio-features",
                "maharshipandya/-spotify-tracks-dataset",
                "theoverman/the-spotify-hit-predictor-dataset",
                "tomsezequielrau/spotify-weekly-top-200-audio-features-20172020"
                ]
    query(datasets)
    