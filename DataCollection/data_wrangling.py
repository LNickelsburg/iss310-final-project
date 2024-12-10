import pandas as pd 
import geopandas as gpd


### LOAD DATA ###

def load_data(clean):
    regional_data = pd.read_csv('DataCollection/data/charts.csv') 
    user_data = pd.read_csv('DataCollection/data/user_listening.csv') 
    track_info = pd.read_csv('DataCollection/data/track_info.csv') 
    track_features = pd.read_csv('DataCollection/data/tracks_features.csv') 
    countries_geo = gpd.read_file('DataCollection/data/countries.geo.json')

    if clean:
        regional_data = clean_regional(regional_data)
        user_data = clean_user(user_data)
        track_info = clean_info(track_info)
        track_features = clean_features(track_features)
        
    return regional_data, user_data, track_info, track_features, countries_geo

def clean_regional(data):
    data.rename(columns={'date': 'chart_date'}, inplace=True)
    data.drop(columns=['rank'], inplace=True)
    return data

def clean_user(data):
    data.rename(columns={'category': 'user_metric'}, inplace=True)
    return data

def clean_info(data):
    data.rename(columns={'date': 'release_date'}, inplace=True)
    data.drop(columns=['name'], inplace=True)
    return data

def clean_features(data):
    data.rename(columns={'id': 'uri'}, inplace=True)
    return data

def clean_geo(data):
    data.rename(columns={'name': 'region'}, inplace=True)
    return data


### MERGE DATA ###

def get_regional(regional, info, features):
    regional = regional.merge(info, on='uri', how='left')
    regional = regional.merge(features, on='uri', how='left')
    return regional

def get_user(user, info, features):
    user = user.merge(info, on='uri', how='left')
    user = user.merge(features, on='uri', how='left')
    return user

def results_to_csv(df, path):
    path = f"C:/Users/lnick/OneDrive/Desktop/Fall 2024/archives as data/final project/DataCollection/{path}"
    if df is not None:
        df.to_csv(path, index=False)

def merge_data():
    regional_data, user_data, track_info, track_features, countries_geo = load_data(clean=True)

    regional_data = get_regional(regional_data, track_info, track_features)
    user_data = get_user(user_data, track_info, track_features)

    results_to_csv(regional_data, 'data/regional_data.csv')
    results_to_csv(user_data, 'data/user_data.csv')


### ACCESS DATA ###

def get_datasets():
    regional_data = pd.read_csv('DataCollection/data/regional_data.csv')
    user_data = pd.read_csv('DataCollection/data/user_data.csv')
    countries_geo = gpd.read_file('DataCollection/data/countries.geo.json')
    countries_geo = clean_geo(countries_geo)
    return regional_data, user_data, countries_geo



if __name__ == '__main__':
    #merge_data()

    regional_data, user_data, countries_geo = get_datasets()
    print(regional_data.columns)


    