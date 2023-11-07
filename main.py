import os
import time
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import requests
import json
import datetime
import pandas as pd
import sqlite3

from authorization_and_token import refresh_token, request_token, request_authorization_code
from error_handling import error_handling

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"

def test_api():
    TOKEN = request_token()
    headers = {
        #"Accept": "application/json",
        #"Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    #print("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp))
    print("")
    print("Testing API")
    r = requests.get(
        "https://api.spotify.com/v1/artists/4Z8W4fKeB5YxbusRsdQVPb",
        headers=headers)
    response = r.json()
    print(response)


def get_client_id_secret_b64():
    pass


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
            #print("Removed tracks with a timestamp that wasn't yesterday")
            #print(df)
            #raise Exception("At least one of the returned songs is not from within the last 24 hours")

    return True


def prepare_data(data):
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

    #print(song_df)
    
    # Validate
    if check_if_valid_data(song_df):
        print("Data is valid, proceed to Load stage")

    #print(song_df)
    dump = song_df.to_json()
    #with open("interfaces.json", "w") as jsonFile:
    #    json.dump(dump, jsonFile, indent=4, sort_keys=False)
    
    with open("interfaces.json", "w") as outfile:
        outfile.write(json.dumps(dump, indent=4))

    write_to_database(song_df)


def write_to_database(song_df):
    print(song_df)
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

    compare_data = pd.read_sql('SeLeCt dIsTiNcT(played_at) fRoM my_played_tracks;', con=engine)
    print(compare_data)

    for entry in compare_data.played_at:
        print(entry)
        song_df = song_df[song_df.played_at != entry]

    print(song_df)
    cursor.execute(sql_query)
    print("Established DB connection")

    try: 
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the Database")

    conn.close()
    print("closed DB connection")


def write_to_json(data):
    # Write method for streamlined JSON storage (like error messages)
    print(data)
    #for track in raw_data["items"]:
    #    print(track["track"]["name"])
    with open("alternative_data.json", "w") as outfile:
        outfile.write(json.dumps(data, indent=4))
    pass


def request_data():
    if not os.path.exists("access_token.json"): 
        print("Requesting initial Token")
        request_token()
    if os.path.exists("access_token.json"):
        print("using current token")
        file = open("access_token.json")
        token_information = json.load(file)
        if datetime.datetime.now().timestamp() >= token_information["Renewal_at"]:
            print("Token expired, requesting new token")
            refresh_token()

    #print(token_information)
    TOKEN = token_information["access_token"]
    #print(TOKEN)
    # Extraction of Data
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    #print("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp))
    print("")
    print("Requesting Recently Played Data")
    r = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp),
        headers=headers)

    data = r.json()
    
    #print(data)
    if "error" in data:
        error_handling(data, "spotify")
    else:
        # Only store the Raw Data if the directory "raw_data" is present, otherwise raw data will not be saved
        if os.path.isdir("raw_data"):
            with open(f"raw_data/query_{datetime.datetime.now().timestamp()}.json", "w") as outfile:
                outfile.write(json.dumps(data, indent=4))
        prepare_data(data)
        write_to_json(data)
        print("Got the Data, next query in 1 Hour")
        start(1)


def start(state):
    if state == 2:
        time.sleep(600)
    elif state == 1: 
        time.sleep(3600)
    request_data()


if __name__ == "__main__":
    if os.path.exists("authorization.json"):
        start(0)
        #test_api()
    else:
        print("You have to generate an Authorization code first: ")
        print("https://developer.spotify.com/documentation/web-api/tutorials/code-flow")
        request_authorization_code()

    # write token+requested time to json file -> only request new token if expired
