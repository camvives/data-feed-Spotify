from requests.sessions import RequestsCookieJar
import sqlalchemy as sq
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
import datetime
import sqlite3
import config
import sys

# Get Spotify Token from: https://developer.spotify.com/console/get-recently-played/
DATATBASE_LOCATION = config.DATATBASE_LOCATION
USER_ID = config.USER_ID
TOKEN = config.TOKEN

def is_valid_data(dataframe: pd.DataFrame) -> bool: 
    ''' Checks if the data on the DataFrame is valid to store'''

    # Checks if dataframe is empty
    if dataframe.empty:
        print("No songs downloaded. Finishing execution")
        return False
    
    # Primary Key Check
    if pd.Series(dataframe["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")
    
    # Checks for nulls
    if dataframe.isnull().values.any():
        raise Exception("Null value found")

    # Check that all timestamps are placed in the last 24 hours
    today = datetime.datetime.now()
    today = today.replace(microsecond=0)
    yesterday = today - datetime.timedelta(days=1)
    yesterday = yesterday.replace(microsecond=0)

    timestamps = dataframe["timestamp"].tolist()
    for timestamp in timestamps:
        song_time = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        if not yesterday < song_time < today:
            raise Exception("At least one of the returned songs does not come from within the last 24 hours")
    
    return True

if __name__ =="__main__":

    # Required request parameters
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    # timedelta express the difference between the two datetimes instances to microsecond resolution.
    # timestamp returns UNIX time (system for describing a point in time) as float
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_timestamp),headers = headers)

    # Exits the program with a message in case of request errors   
    if r.status_code != 200:
        print("Request error:", r)
        sys.exit(1)
    
    # Transforms the request to json-encoded content
    data = r.json()
    
    song_names = []
    artists_names = []
    played_at_list = []
    timestamps = []

    # Getting specific data from each song
    # Reference: https://developer.spotify.com/documentation/web-api/reference/#endpoint-get-recently-played
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artists_names.append(song["track"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])

        arg_hour = int(song["played_at"][11:13]) - 3 #time diference between Argentina and Spotify Data
        timestamps.append(song["played_at"][0:10] + " " + str(arg_hour)+song["played_at"][13:19])

    # Converts lists into a dictionary     
    song_dict = {
        "song_name" : song_names,
        "artist_name" : artists_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    # Using pandas converts the dictionary into a DataFrame (distributed collection of data organized into named columns)
    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

    # Validate
    try:
        if is_valid_data(song_df):
            print("Data is valid")
    except Exception as er: print(er)

    # Load
    engine = sq.create_engine(DATATBASE_LOCATION)

    if not sq.inspect(engine).has_table("my_played_tracks"):  # If table doesn't exist.
        # Create a metadata instance
        metadata = sq.MetaData(engine)
        # Declare a table
        table = sq.Table('my_played_tracks',metadata,
                    sq.Column('song_name', sq.String),
                    sq.Column('artist_name', sq.String),
                    sq.Column('played_at', sq.String),
                    sq.Column('timestamp', sq.String, primary_key=True))
        # Create table
        metadata.create_all()

    try:
        conn = sqlite3.connect("my_played_tracks.sqlite")
        print('Database connection established')
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append') # Don't use pandas indexes
        print("Done.")
    except Exception as er: print(er)

    conn.close()
    