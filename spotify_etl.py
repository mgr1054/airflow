import sys
import os 
sys.path.append("C\\wsl$\\Ubuntu\\home\\gress\\airflow")
sys.path.insert(0, os.getcwd())
import pandas as pd
import requests
from datetime import date
from plugins.create import writeIntoDB

def run_spotify_etl():
    # Valid for one hour
    TOKEN = 'BQD0X62SZ0qq_YDTbzeaKW1RCoyH5U62z6jrKZV5ynrCzmN07ipzwYC46Vz-CjCMtKWEbeK6llHmfXG5TgBWS9TSe1YuXhikPTi2iKzjHyyhxuH5Q1A5cqemVt_0J3Mk8mYKivhmZ52bjn_HcAvdXJGJvgNPuBYj5MxHuA0_7wdYqCQiZyIbpnhn6qcuoDBthW3IGjc3N4-45bItdCPW9_kg'
    # Global Charts
    PLAYLIST_ID = '37i9dQZEVXbNG2KDcFcKOF'
    
    # Extract part of the ETL process
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    # Download all charts    
    r = requests.get("https://api.spotify.com/v1/playlists/{playlist_id}/tracks".format(playlist_id=PLAYLIST_ID), headers = headers)

    data = r.json()

    place = []
    song_names = []
    artist_names = []
    album_name = []
    album_release = []
    song_popularity = []
    timestamp = []

    # Extracting only the relevant bits of data from the json object
    place_counter = 1      
    for song in data["items"]:
        place.append(place_counter)
        place_counter += 1
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["artists"][0]["name"])
        album_name.append(song["track"]["album"]["name"])
        album_release.append(song["track"]["album"]["release_date"])
        song_popularity.append(song["track"]["popularity"])
        timestamp = date.today()

        
    # Prepare a dictionary in order to turn it into a pandas dataframe below       
    song_dict = {
        "place" : place,
        "song_name" : song_names,
        "artist_name": artist_names,
        "album_name" : album_name,
        "album_release" : album_release,
        "song_popularity" : song_popularity,
        "timestamp": timestamp
    }

    song_df = pd.DataFrame(song_dict, columns = ["place", "song_name", "artist_name", "album_name", "album_release", "song_popularity", "timestamp"])
    print(song_df)
    # writeIntoDB(song_df)

run_spotify_etl()