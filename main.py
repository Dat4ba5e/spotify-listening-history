import sqlalchemy
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import date, datetime
import datetime
import pandas as pd
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "f6frc1m72da9a2r0chb3vj85w"
# get token from: https://developer.spotify.com/console/get-recently-played/?limit=&after=&before=
TOKEN = "BQBqoJ_md2jb-vQYO26HKk06mFzQo2L0B5KniTLa-rmbUyA5RcnUToPTeldlTN42XMcTUySe7XrEYmiUC2PDHCWOOYJvKK0J_WKYfbwkr_L5UY_knkIqVb2MCNHhzQ4EM3UG_RT2LDjNPgoE7iZHgcRwGZw9NwO-byrR1Z6g"

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if DataFrame is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    #Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")

    #check for nulls
    if df.isnull().values.any():
        raise Exception("Null value Found")

    # Check that timestamps are of yesterdays date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0,minute=0,second=0,microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            df = df[df.timestamp == yesterday]
            print("Removed tracks with a timestamp that wasn't yesterday")
            print(df)
            #raise Exception("At least one of the returned songs is not from within the last 24 hours")

    return True



if __name__ == "__main__":

    # Extraction of Data

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp),
        headers=headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])

    print(song_df)
    
    # Validate
    if check_if_valid_data(song_df):
        print("Data is valid, proceed to Load stage")

    print(song_df)

    # Load to DB
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Established DB connection")

    try: 
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the Database")

    conn.close()
    print("closed DB connection")
