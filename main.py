import os
import time
import string
import random
import webbrowser
import base64
import sqlalchemy
import urllib.parse
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import date, datetime
import datetime
import pandas as pd
import sqlite3
from http.client import responses
import re
import pytz

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"

def test_api():
    # make get_token method
    TOKEN = get_token()
    headers = {
        #"Accept": "application/json",
        #"Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    #print("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp))
    print_status_message("Testing API")
    r = requests.get(
        "https://api.spotify.com/v1/artists/4Z8W4fKeB5YxbusRsdQVPb",
        headers=headers)
    return r.status_code

# first thing to do: paste authorization code in file: authorization.json (will be generated while on first run of the program)
# authorization code: after accepting on the website in the URL after "code="
def request_authorization_code():
    file = open("authentication.json")
    credentials = json.load(file)

    # Recommended for protection against attacks such as cross-site request forgery. See RFC-6749.
    # used for "state" field
    random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
    #print(random_string)
    url = "https://accounts.spotify.com/authorize?"

    params = {
    "response_type": "code",
    "client_id": credentials["spotify"]["client_id"],
    "scope": credentials["spotify"]["scope"],
    "redirect_uri": credentials["spotify"]["redirect_uri"],
    "state": random_string
    }

    webbrowser.open("https://accounts.spotify.com/authorize?" + urllib.parse.urlencode(params))
    
    # implement function to automatically filter out the "state"

    r = requests.get(url, params=params)
    #print(r)
    #print(type(r))

    template = {"authorization_key": ""}
    with open("authorization.json", "w") as outfile:
        outfile.write(json.dumps(template, indent=4))


def get_client_id_secret_b64():
    pass


def request_token():
    authentication_file = open("authentication.json")
    authorization_file = open("authorization.json")
    authentication_data = json.load(authentication_file)
    authorization_data = json.load(authorization_file)

    client_id = authentication_data["spotify"]["client_id"]
    client_secret = authentication_data["spotify"]["client_secret"]
    mix = f"{client_id}:{client_secret}"
    #print(mix)
    mix_bytes = mix.encode("ascii")
    mix_b64 = base64.urlsafe_b64encode(mix_bytes).decode("ascii")[:-1]
    #print(mix_b64)

    headers = {
        "Authorization": f"Basic {mix_b64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    #print("................")
    #print(authorization_data["authorization_key"])
    #print("................")

    param = {
        "grant_type": "authorization_code",
        "code": authorization_data["authorization_key"],
        "redirect_uri": authentication_data["spotify"]["redirect_uri"]
    }

    url = "https://accounts.spotify.com/api/token?"
    r = requests.post(url=url, headers=headers, params=param)
    response = r.json()

    #print(response)
    response.update({"Renewal_at": datetime.datetime.now().timestamp() + response["expires_in"]-10} )
    with open("access_token.json", "w") as outfile:
        outfile.write(json.dumps(response, indent=4))
    #return response["access_token"]


def refresh_token():
    file = open("access_token.json")
    data = json.load(file)
    file_auth = open("authentication.json")
    data2 = json.load(file_auth)

    client_id = data2["spotify"]["client_id"]
    client_secret = data2["spotify"]["client_secret"]
    mix = f"{client_id}:{client_secret}"
    #print(mix)
    mix_bytes = mix.encode("ascii")
    mix_b64 = base64.urlsafe_b64encode(mix_bytes).decode("ascii")[:-1]
    
    refresh_token = data["refresh_token"]
    #print(refresh_token)
    client_id = data2["spotify"]["client_id"]
    url = "https://accounts.spotify.com/api/token?"
    param = {
        "grant_type": 'refresh_token',
        "refresh_token": refresh_token,
        #"client_id": client_id
    }

    headers = { "Content-Type": "application/x-www-form-urlencoded", "Authorization": f"Basic {mix_b64}" }

    r = requests.post(url=url, params=param, headers=headers)
    response = r.json()
    #print(response)

    if "refresh_token" not in response:
        response.update({ "refresh_token": refresh_token })

    response.update({"Renewal_at": datetime.datetime.now().timestamp() + response["expires_in"]-10} )
    with open("access_token.json", "w") as outfile:
        outfile.write(json.dumps(response, indent=4))


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


def error_handling(error, source):
    if type(error) is int:
        print_status_message(f"Received error code {error} - {responses[error]}", "error")
        error = { 
            "error": 
                {
                    "status": error,
                    "message": responses[error],
                    "time": []
                }  
            }
        
    api_test_result = test_api()
    print_status_message(f"API Status: {api_test_result} - {responses[api_test_result]}", "error")

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
        print_status_message("Requesting new Token", "error")
        
        with open("token_log.txt", "a") as outfile:
            outfile.write("Requesting new Token at time {time}".format(time=datetime.datetime.now().timestamp()))
        start(0)
    else:
        print_status_message("Encountered an error, trying again in 10 Minutes", "error")
        start(2)


def print_status_message(message: str, msg_type = "status"):
    if msg_type == "status":
        replace_sign = "-"
    else:
        replace_sign = "!"
    lines = re.sub(".", replace_sign, message)
    print("")
    print(lines)
    print(message)
    print(lines)
    print("")


def prepare_data(data):
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
    played_at_unix_list = []
    
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
        played_at_unix_list.append(convert_timestamp(song["played_at"]))

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps,
        "timestamp_unix": played_at_unix_list
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp", "timestamp_unix"])
    song_df = song_df.sort_values(by="timestamp_unix", ascending=True, na_position="first")

    #print(song_df)
    
    # Validate
    if check_if_valid_data(song_df):
        print_status_message("Data is valid, proceed to Load stage")

    #print(song_df)
    dump = song_df.to_json()
    #with open("interfaces.json", "w") as jsonFile:
    #    json.dump(dump, jsonFile, indent=4, sort_keys=False)
    
    with open("interfaces.json", "w") as outfile:
        outfile.write(json.dumps(dump, indent=4))

    write_to_database(song_df)


def write_to_database(song_df):
    #print(song_df)
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    print("Creating Table")
    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        timestamp_unix VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print_status_message("Established DB connection")
    
    compare_data = pd.read_sql('SeLeCt dIsTiNcT(played_at) fRoM my_played_tracks;', con=engine)
    #print(compare_data)

    for entry in compare_data.played_at:
        #print(entry)
        song_df = song_df[song_df.played_at != entry]
    try:   
        print("------")
        print(song_df)
        print("------")
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print_status_message("Data already exists in the Database")

    conn.close()
    print_status_message("closed DB connection")


def write_to_json(data):
    # Write method for streamlined JSON storage (like error messages)
    #print(data)
    #for track in raw_data["items"]:
    #    print(track["track"]["name"])
    with open("alternative_data.json", "w") as outfile:
        outfile.write(json.dumps(data, indent=4))
    pass


def normalize_raw_data():
    # Keep the RAW data from the requests in it's original form but delete duplicates
    raw_data_directory = "raw_data"
    if os.path.exists(raw_data_directory): 
        if os.path.isdir(raw_data_directory):
            for filename in os.listdir(raw_data_directory):
                with open(os.path.join(raw_data_directory, filename)) as f:
                    data = json.load(f)
    pass


def normalize_json_data():
    pass


def convert_timestamp(date_string):
    if "." not in date_string:
        date_string = date_string[:len(date_string)-1] + ".0Z"
    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    date_object = datetime.datetime.strptime(date_string, date_format)
    return int(date_object.replace(tzinfo=pytz.UTC).timestamp() * 1000)


def get_token():
    if not os.path.exists("access_token.json"): 
        print_status_message("Requesting initial Token")
        request_token()
    if os.path.exists("access_token.json"):
        print_status_message("using current token")
        file = open("access_token.json")
        token_information = json.load(file)
        if datetime.datetime.now().timestamp() >= token_information["Renewal_at"]:
            print("Token expired, requesting new token")
            refresh_token()
    return token_information["access_token"]


def request_song_data():
    #print(token_information)
    TOKEN = get_token()
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
    print_status_message("Requesting Recently Played Data")
    r = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp),
        headers=headers)
    
    if r.status_code > 202:
        #print("Kritisch")
        error_handling(r.status_code, "spotify")
    
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
        print_status_message("Got the Data, next query in 1 Hour")
        start(1)


def start(state):
    if state == 2:
        time.sleep(600)
    elif state == 1: 
        time.sleep(3600)
    request_song_data()


if __name__ == "__main__":
    if os.path.exists("authorization.json"):
        start(0)
        #test_api()
    else:
        print("")
        print("You have to generate an Authorization code first: ")
        print("https://developer.spotify.com/documentation/web-api/tutorials/code-flow")
        request_authorization_code()

    # write token+requested time to json file -> only request new token if expired
