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
    TOKEN = 'BQBvxUY1zhHH09dJ5baTgvJr6W8wavbpZenHekkN2YYbEemTx16kios6glL5nYq6PXV_koQ6il0fCd7G2ya4WA_TvmQgLGmdcr9TUg2s7XNxAEvcucVUnw9bxQXqCi2NuPMLnyAhiHL_sRMSUI9kJUNCLAgrDY7W5eWLCrBp7sa4gVcTsWdudHyxdkGv62M1hJfMHe_-HeeFLyQrfRZviv6I'
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
    writeIntoDB(song_df)

run_spotify_etl()