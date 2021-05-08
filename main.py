from requests.sessions import RequestsCookieJar
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
import datetime
import sqlite3
import config
import sys

DATATBASE_LOCATION = config.DATATBASE_LOCATION
USER_ID = config.USER_ID
TOKEN = config.TOKEN

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
        timestamps.append(song["played_at"][0:10])

    # Converts lists into a dictionary     
    song_dict = {
        "song_name" : song_names,
        "artist_name" : artists_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    # Using pandas converts the dictionary into a DataFrame (distributed collection of data organized into named columns)
    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

    print(song_df)