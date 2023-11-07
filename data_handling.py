import os
import requests
import datetime
import json
import pandas as pd

from authorization_and_token import request_token, refresh_token
from error_handling import error_handling
from main import start


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