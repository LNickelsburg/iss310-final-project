
import pandas as pd
from collections import Counter
import ast

### HELPERS ###

def label_counter(label_series):
    all_labels = []
    for label in label_series.dropna():
        try:
            # Parse the string to extract genres as a list
            label_list = ast.literal_eval(label) if isinstance(label, str) else label
            if isinstance(label_list, list):
                all_labels.extend(label_list)
        except (ValueError, SyntaxError):
            continue
    return dict(Counter(all_labels))

def to_csv(data, path):
    data.to_csv(path, index=False)


### AGGREGATION ###

def agg_regional_info(data):
    print('Cleaning data...')
    data = data.drop(columns=['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature'])
    data['release_date'] = pd.to_datetime(data['release_date'], errors='coerce')
    data['release_date'] = data['release_date'].map(lambda x: x.timestamp() if pd.notnull(x) else None)

    print('Aggregating...')
    agg_data = data.groupby(['region', 'chart_date']).agg(
        num_tracks=('uri', 'count'),
        avg_popularity=('popularity', 'mean'),
        avg_release_date=('release_date', 'mean')
    ).reset_index()

    print('Counting genres...')
    counts = (
        data.groupby(['region', 'chart_date'])['genre']
        .apply(lambda x: {'genre_counts': label_counter(x)})
        .reset_index()
    )
    counts = counts.drop(columns=['level_2'])
    counts.rename(columns={'genre': 'counts'}, inplace=True)

    print('Merging data...')
    agg_data = pd.merge(agg_data, counts, on=['region', 'chart_date'], how='left')
    
    print('Converting timestamps...')
    agg_data['avg_release_date'] = pd.to_datetime(
        agg_data['avg_release_date'], unit='s', errors='coerce'
    ).dt.date
    print('Done.')

    return agg_data




if __name__ == '__main__':
    print('Reading data...')
    regional_info = pd.read_csv('DataCollection/data/regional_info.csv')
    regional_features = pd.read_csv('DataCollection/data/regional_features.csv')
    user_info = pd.read_csv('DataCollection/data/user_info.csv')
    user_features = pd.read_csv('DataCollection/data/user_features.csv')

    print('Calling aggregator...')
    regional_info_agg = agg_regional_info(regional_info)
    to_csv(regional_info_agg,'Exploration/aggregated_data/agg_regional_info.csv')
    