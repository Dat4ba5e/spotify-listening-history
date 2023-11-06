import os
import time
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import date, datetime
import datetime
import pandas as pd
import sqlite3

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

def request_token():
    file = open("authentication.json")
    data = json.load(file)

    #print(data["spotify"]["client_id"])
    

    client_id = data["spotify"]["client_id"]
    client_secret = data["spotify"]["client_secret"]
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    r = requests.post(
        "https://accounts.spotify.com/api/token?grant_type=client_credentials&client_id={id}&client_secret={secret}&scope=user-read-recently-played".format(id=client_id, secret=client_secret), headers=headers)
    response = r.json()

    print(response)
    response.update({"Renewal_at": datetime.datetime.now().timestamp() + response["expires_in"]-10} )
    with open("access_token.json", "w") as outfile:
        outfile.write(json.dumps(response, indent=4))
    #return response["access_token"]


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


def error_handling(error, source):
    if os.path.exists("error_log.json") and os.stat("error_log.json").st_size != 0:
        # check if the error_log.json file exist and if it is not empty
        file = open("error_log.json")
        data = json.load(file)
    else:
        # initialize the Dict that will become the JSON file if it doesn't exist yet
        data = { "spotify": [], "youtube": []}

    counter = 0
    for err in data[source]:
        if err["error"]["status"] == error["error"]["status"]:
            err["error"]["time"].append(int(datetime.datetime.now().timestamp()))
            counter += 1

    #print(counter)
    if counter == 0:
        error["error"].update({"time": [int(datetime.datetime.now().timestamp())]})
        data[source].append(error)

    with open("error_log.json", "w") as outfile:
        outfile.write(json.dumps(data, indent=4))

    if error["error"]["status"] == 401:
        print("Requesting new Token")
        with open("token_log.txt", "a") as outfile:
            outfile.write("Requesting new Token at time {time}".format(time=datetime.datetime.now().timestamp()))
        start(0)
    else:
        print("Encountered an error, trying again in 10 Minutes")
        start(2)


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

    print(song_df)
    
    # Validate
    if check_if_valid_data(song_df):
        print("Data is valid, proceed to Load stage")

    print(song_df)
    dump = song_df.to_json()
    with open("interfaces.json", "w") as jsonFile:
        json.dump(dump, jsonFile, indent=4, sort_keys=False)
    
    write_to_database(song_df)


def write_to_database(song_df):
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
            request_token()

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

    print(data)
    if "error" in data:
        error_handling(data, "spotify")
    else:
        prepare_data(data)
        print("Got the Data, next query in 1 Hour")
        start(1)

    


def start(state):
    if state == 2:
        time.sleep(2)
    elif state == 1: 
        time.sleep(3600)
    request_data()


if __name__ == "__main__":
    #test_api()
    # new_token()
    start(0)

    # write token+requested time to json file -> only request new token if expired
