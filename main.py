import sqlalchemy
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import pandas as pd
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "f6frc1m72da9a2r0chb3vj85w"
TOKEN = "BQAqGP_05MiGt3PILEmcMK3rHZT5Q-URHS9cyvNRNzfLI98k5sT-vkWEZggT-AmSDweEO2xvDzpI8Kma31mdwEwuOHmdVwmQWW2qtvpqnfruYKs9okWBwvAU4OeXlXAVrxz68KGuCQw6Yaj1OUoGYGu1-aCYxcpJFjjnfd7l"

if __name__ == "__main__":

    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()

    print(data)

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        print(song_names)
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        print(artist_names)
        played_at_list.append(song["played_at"])
        print(played_at_list)
        timestamps.append(song["played_at"][0:10])
        print(timestamps)

    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict,columns = ["song_name", "artist_name", "played_at", "timestamp"])

    print(song_df)
