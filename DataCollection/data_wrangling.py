import pandas as pd 
import geopandas as gpd


### LOAD DATA ###

def load_data(clean):
    regional_data = pd.read_csv('DataCollection/data/raw/charts.csv') 
    user_data = pd.read_csv('DataCollection/data/raw/user_listening.csv') 
    track_info = pd.read_csv('DataCollection/data/modified/track_info.csv') 
    track_features = pd.read_csv('DataCollection/data/modified/tracks_features.csv') 
    countries_geo = gpd.read_file('DataCollection/data/raw/geospatial/countries.geo.json')

    if clean:
        regional_data = clean_regional(regional_data)
        user_data = clean_user(user_data)
        track_info = clean_info(track_info)
        track_features = clean_features(track_features)
        countries_geo = clean_geo(countries_geo)
        
    return regional_data, user_data, track_info, track_features, countries_geo

def clean_regional(data):
    data.rename(columns={'date': 'chart_date'}, inplace=True)
    return data

def clean_user(data):
    data.rename(columns={'category': 'user_metric'}, inplace=True)

    return data

def clean_info(data):
    data.rename(columns={'date': 'release_date'}, inplace=True)
    data.drop(columns=['name'], inplace=True)
    data.drop_duplicates(subset=['uri'], inplace=True)
    return data

def clean_features(data):
    data.rename(columns={'id': 'uri'}, inplace=True)
    data.drop_duplicates(subset=['uri'], inplace=True)
    return data

def clean_geo(data):
    data.rename(columns={'name': 'region'}, inplace=True)
    return data


### MERGE DATA ###

def get_regional(regional, info, features):
    
    regional_info = regional.merge(info, on='uri', how='left')
    regional_info = regional_info.merge(features, on='uri', how='left')
    regional_info.drop_duplicates(subset=['chart_date', 'region', 'uri'], inplace=True)


    regional_features = regional.merge(features, on='uri', how='inner')
    regional_features = regional_features.merge(info, on='uri', how='left')
    regional_features.drop_duplicates(subset=['chart_date', 'region', 'uri'], inplace=True)

    return regional_info, regional_features

def get_user(user, info, features):

    user_info = user.merge(info, on='uri', how='left')
    user_info = user_info.merge(features, on='uri', how='left')
    user_info.drop_duplicates(subset=['user_metric', 'uri'], inplace=True)

    user_features = user.merge(features, on='uri', how='inner')
    user_features = user_features.merge(info, on='uri', how='left')
    user_features.drop_duplicates(subset=['user_metric', 'uri'], inplace=True)

    return user_info, user_features

def results_to_csv(df, path):
    path = f"C:/Users/lnick/OneDrive/Desktop/Fall 2024/archives as data/final project/DataCollection/{path}"
    if df is not None:
        df.to_csv(path, index=False)

def merge_data():
    print('Loading data...')
    regional_data, user_data, track_info, track_features, countries_geo = load_data(clean=True)

    print('Merging data...')
    regional_info, regional_features = get_regional(regional_data, track_info, track_features)
    user_info, user_features = get_user(user_data, track_info, track_features)

    print('Saving data...')
    results_to_csv(regional_info, 'data/regional_info.csv')
    results_to_csv(regional_features, 'data/regional_features.csv')
    results_to_csv(user_info, 'data/user_info.csv')
    results_to_csv(user_features, 'data/user_features.csv')
    


### ACCESS DATA ###

def get_datasets():
    regional_info = pd.read_csv('DataCollection/data/regional_info.csv')
    regional_features = pd.read_csv('DataCollection/data/regional_features.csv')
    user_info = pd.read_csv('DataCollection/data/user_info.csv')
    user_features = pd.read_csv('DataCollection/data/user_features.csv')

    countries_geo = gpd.read_file('DataCollection/data/raw/geospatial/countries.geo.json')
    countries_geo = clean_geo(countries_geo)

    return regional_info, regional_features, user_info, user_features, countries_geo



if __name__ == '__main__':
    merge_data()


    