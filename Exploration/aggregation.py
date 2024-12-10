
import pandas as pd
from collections import Counter
import ast
import os

### HELPERS ###

def label_counter(label_series):
    all_labels = []
    for label in label_series.dropna():
        label = str(label)
        try:
            # Parse the string to extract lists or count individual values
            label_list = ast.literal_eval(label) if isinstance(label, str) else label
            if isinstance(label_list, list):
                all_labels.extend(label_list)
            else:
                all_labels.append(label_list)  # Append non-list values as-is
        except (ValueError, SyntaxError):
            all_labels.append(label)  # Treat as a single value if parsing fails
    return dict(Counter(all_labels))

def to_csv(data, path):
    data.to_csv(path, index=False)


### AGGREGATION ###

def agg_regional_info(data):
    data = data.drop(columns=['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature'])
    data['release_date'] = pd.to_datetime(data['release_date'], errors='coerce')
    data['release_date'] = data['release_date'].map(lambda x: x.timestamp() if pd.notnull(x) else None)

    agg_data = data.groupby(['scope', 'region', 'chart_date']).agg(
        num_tracks=('uri', 'count'),
        avg_popularity=('popularity', 'mean'),
        avg_release_date=('release_date', 'mean')
    ).reset_index()

    genre_counts = (data.groupby(['scope', 'region', 'chart_date'])['genre'].apply(lambda x: {'genre_counts': label_counter(x)}).reset_index())
    genre_counts = genre_counts.drop(columns=['level_3'])
    genre_counts.rename(columns={'genre': 'genre_counts'}, inplace=True)
    agg_data = pd.merge(agg_data, genre_counts, on=['region', 'chart_date'], how='left')
    
    agg_data['avg_release_date'] = pd.to_datetime(
        agg_data['avg_release_date'], unit='s', errors='coerce'
    ).dt.date

    return agg_data

def agg_regional_features(data):
    data['release_date'] = pd.to_datetime(data['release_date'], errors='coerce')
    data['release_date'] = data['release_date'].map(lambda x: x.timestamp() if pd.notnull(x) else None)

    agg_data = data.groupby(['scope', 'region', 'chart_date']).agg(
        num_tracks=('uri', 'count'),
        avg_popularity=('popularity', 'mean'),
        avg_release_date=('release_date', 'mean'),
        avg_danceability=('danceability', 'mean'),
        avg_energy=('energy', 'mean'),
        avg_loudness=('loudness', 'mean'),
        avg_speechiness=('speechiness', 'mean'),
        avg_acousticness=('acousticness', 'mean'),
        avg_instrumentalness=('instrumentalness', 'mean'),
        avg_liveness=('liveness', 'mean'),
        avg_valence=('valence', 'mean'),
        avg_tempo=('tempo', 'mean')
    ).reset_index()

    key_counts = (data.groupby(['scope', 'region', 'chart_date'])['key'].apply(lambda x: {'key_counts': label_counter(x)}).reset_index())
    key_counts = key_counts.drop(columns=['level_3'])
    key_counts.rename(columns={'key': 'key_counts'}, inplace=True)
    mode_counts = (data.groupby(['scope', 'region', 'chart_date'])['mode'].apply(lambda x: {'mode_counts': label_counter(x)}).reset_index())
    mode_counts = mode_counts.drop(columns=['level_3'])
    mode_counts.rename(columns={'mode': 'mode_counts'}, inplace=True)
    time_signature_counts = (data.groupby(['scope', 'region', 'chart_date'])['time_signature'].apply(lambda x: {'time_signature_counts': label_counter(x)}).reset_index())
    time_signature_counts = time_signature_counts.drop(columns=['level_3'])
    time_signature_counts.rename(columns={'time_signature': 'time_signature_counts'}, inplace=True)
    genre_counts = (data.groupby(['scope', 'region', 'chart_date'])['genre'].apply(lambda x: {'genre_counts': label_counter(x)}).reset_index())
    genre_counts = genre_counts.drop(columns=['level_3'])
    genre_counts.rename(columns={'genre': 'genre_counts'}, inplace=True)
    agg_data = pd.merge(agg_data, key_counts, on=['scope', 'region', 'chart_date'], how='left')
    agg_data = pd.merge(agg_data, mode_counts, on=['scope', 'region', 'chart_date'], how='left')
    agg_data = pd.merge(agg_data, time_signature_counts, on=['scope', 'region', 'chart_date'], how='left')
    agg_data = pd.merge(agg_data, genre_counts, on=['scope', 'region', 'chart_date'], how='left')

    agg_data['avg_release_date'] = pd.to_datetime(
        agg_data['avg_release_date'], unit='s', errors='coerce'
    ).dt.date

    return agg_data

def agg_user_info(data):
    data = data.drop(columns=['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature'])
    data['release_date'] = pd.to_datetime(data['release_date'], errors='coerce')
    data['release_date'] = data['release_date'].map(lambda x: x.timestamp() if pd.notnull(x) else None)

    agg_data = data.groupby(['user_metric']).agg(
        num_tracks=('uri', 'count'),
        avg_popularity=('popularity', 'mean'),
        avg_release_date=('release_date', 'mean')
    ).reset_index()

    genre_counts = (data.groupby(['user_metric'])['genre'].apply(lambda x: {'genre_counts': label_counter(x)}).reset_index())
    genre_counts = genre_counts.drop(columns=['level_1'])
    genre_counts.rename(columns={'genre': 'genre_counts'}, inplace=True)
    agg_data = pd.merge(agg_data, genre_counts, on=['user_metric'], how='left')
    
    agg_data['avg_release_date'] = pd.to_datetime(
        agg_data['avg_release_date'], unit='s', errors='coerce'
    ).dt.date

    return agg_data

def agg_user_features(data):
    data['release_date'] = pd.to_datetime(data['release_date'], errors='coerce')
    data['release_date'] = data['release_date'].map(lambda x: x.timestamp() if pd.notnull(x) else None)

    agg_data = data.groupby(['user_metric']).agg(
        num_tracks=('uri', 'count'),
        avg_popularity=('popularity', 'mean'),
        avg_release_date=('release_date', 'mean'),
        avg_danceability=('danceability', 'mean'),
        avg_energy=('energy', 'mean'),
        avg_loudness=('loudness', 'mean'),
        avg_speechiness=('speechiness', 'mean'),
        avg_acousticness=('acousticness', 'mean'),
        avg_instrumentalness=('instrumentalness', 'mean'),
        avg_liveness=('liveness', 'mean'),
        avg_valence=('valence', 'mean'),
        avg_tempo=('tempo', 'mean')
    ).reset_index()

    key_counts = (data.groupby(['user_metric'])['key'].apply(lambda x: {'key_counts': label_counter(x)}).reset_index())
    key_counts = key_counts.drop(columns=['level_1'])
    key_counts.rename(columns={'key': 'key_counts'}, inplace=True)
    mode_counts = (data.groupby(['user_metric'])['mode'].apply(lambda x: {'mode_counts': label_counter(x)}).reset_index())
    mode_counts = mode_counts.drop(columns=['level_1'])
    mode_counts.rename(columns={'mode': 'mode_counts'}, inplace=True)
    time_signature_counts = (data.groupby(['user_metric'])['time_signature'].apply(lambda x: {'time_signature_counts': label_counter(x)}).reset_index())
    time_signature_counts = time_signature_counts.drop(columns=['level_1'])
    time_signature_counts.rename(columns={'time_signature': 'time_signature_counts'}, inplace=True)
    genre_counts = (data.groupby(['user_metric'])['genre'].apply(lambda x: {'genre_counts': label_counter(x)}).reset_index())
    genre_counts = genre_counts.drop(columns=['level_1'])
    genre_counts.rename(columns={'genre': 'genre_counts'}, inplace=True)
    agg_data = pd.merge(agg_data, key_counts, on=['user_metric'], how='left')
    agg_data = pd.merge(agg_data, mode_counts, on=['user_metric'], how='left')
    agg_data = pd.merge(agg_data, time_signature_counts, on=['user_metric'], how='left')
    agg_data = pd.merge(agg_data, genre_counts, on=['user_metric'], how='left')

    agg_data['avg_release_date'] = pd.to_datetime(
        agg_data['avg_release_date'], unit='s', errors='coerce'
    ).dt.date

    return agg_data



def aggregate_all():
    regional_info = pd.read_csv('DataCollection/data/regional_info.csv')
    regional_features = pd.read_csv('DataCollection/data/regional_features.csv')
    user_info = pd.read_csv('DataCollection/data/user_info.csv')
    user_features = pd.read_csv('DataCollection/data/user_features.csv')

    regional_info_agg = agg_regional_info(regional_info)
    regional_features_agg = agg_regional_features(regional_features)
    user_info_agg = agg_user_info(user_info)
    user_features_agg = agg_user_features(user_features)

    to_csv(regional_info_agg,'Exploration/aggregated_data/agg_regional_info.csv')
    to_csv(regional_features_agg,'Exploration/aggregated_data/agg_regional_features.csv')
    to_csv(user_info_agg,'Exploration/aggregated_data/agg_user_info.csv')
    to_csv(user_features_agg,'Exploration/aggregated_data/agg_user_features.csv')
    
def get_datasets():
    
    if not os.path.isfile('Exploration/aggregated_data/agg_regional_info.csv'):
        regional_info = pd.read_csv('DataCollection/data/regional_info.csv')
        regional_info_agg = agg_regional_info(regional_info)
        to_csv(regional_info_agg,'Exploration/aggregated_data/agg_regional_info.csv')
    else:
        regional_info = pd.read_csv('Exploration/aggregated_data/agg_regional_info.csv')

    if not os.path.isfile('Exploration/aggregated_data/agg_regional_features.csv'):
        regional_features = pd.read_csv('DataCollection/data/regional_features.csv')
        regional_features_agg = agg_regional_features(regional_features)
        to_csv(regional_features_agg,'Exploration/aggregated_data/agg_regional_features.csv')
    else:
        regional_features = pd.read_csv('Exploration/aggregated_data/agg_regional_features.csv')

    if not os.path.isfile('Exploration/aggregated_data/agg_user_info.csv'):
        user_info = pd.read_csv('DataCollection/data/user_info.csv')
        user_info_agg = agg_user_info(user_info)
        to_csv(user_info_agg,'Exploration/aggregated_data/agg_user_info.csv')
    else:
        user_info = pd.read_csv('Exploration/aggregated_data/agg_user_info.csv')

    if not os.path.isfile('Exploration/aggregated_data/agg_user_features.csv'):
        user_features = pd.read_csv('DataCollection/data/user_features.csv')
        user_features_agg = agg_user_features(user_features)
        to_csv(user_features_agg,'Exploration/aggregated_data/agg_user_features.csv')
    else:
        user_features = pd.read_csv('Exploration/aggregated_data/agg_user_features.csv')

    return regional_info, regional_features, user_info, user_features


if __name__ == '__main__':
    aggregate_all()