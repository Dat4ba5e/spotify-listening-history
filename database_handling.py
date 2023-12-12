import sqlalchemy, sqlite3, requests, json, os
from sqlite3 import OperationalError
import pandas as pd
import datetime

from main import print_status_message, get_token, convert_timestamp

DATABASE_FILE = "'my_played_tracks_test.sqlite'"
DATABASE_CREATION_SCRIPT = "database_creation_script.sql"
DATABASE_LOCATION = "sqlite:///my_played_tracks_test.sqlite"
engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('my_played_tracks_test.sqlite')
CURSOR = conn.cursor()


def execute_sql_script(filename):
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()
    fd = open(filename, 'r')
    sql_file = fd.read()
    fd.close()

    commands = sql_file.split(";")

    for command in commands:
        try:
            print("executing command: " + command)
            cursor.execute(command)
        except OperationalError as msg:
            print_status_message("Command skipped: ", msg)

    
def create_database():
    execute_sql_script(DATABASE_CREATION_SCRIPT)


def write_to_database(df, table):
    create_database()
    print_status_message("Established DB connection")

    primary_key = list(df)[0]
    compare_data = pd.read_sql(f'SeLeCt {primary_key} fRoM {table};', con=engine)
    print(compare_data)
    #print(compare_data)

    for entry in compare_data[primary_key]:
        #print(entry)
        df = df[df[primary_key] != entry]
    try:   
        print("------")
        print(df)
        print("------")
        df.to_sql(table, engine, index=False, if_exists='append')
    except:
        print_status_message("Data already exists in the Database")

    conn.close()
    print_status_message("closed DB connection")


def request_data(url, headers = False):
    #url = "https://api.spotify.com/v1/artists/3o2dn2O0FCVsWDFSh8qxgG"

    TOKEN = get_token()
    if not headers:
        headers = {
            #"Accept": "application/json",
            #"Content-Type": "application/json",
            "Authorization": f"Bearer {TOKEN}"
        }
    if headers["Authorization"] == "":
        headers["Authorization"] = f"Bearer {TOKEN}"

    print(headers)
    print_status_message("Testing API")
    r = requests.get(
        url,
        headers=headers)
    if r.status_code <= 202:
        print(r.status_code)
        #if os.path.isdir("raw_data"):
        #    with open(f"raw_data/query_{datetime.datetime.now().timestamp()}.json", "w") as outfile:
        #        outfile.write(json.dumps(r.json(), indent=4))
        return r.json()
    else:
        print_status_message("Information could not be fetched")
        # TODO: error handling
        return
    


def get_artists(artist_ids: list):
    api_hook = "https://api.spotify.com/v1/artists?ids="
    query = ""
    for id in artist_ids:
        query += id + ","
    query = query.rstrip(",")
    print(query)
    data = request_data(api_hook+query)

    artist_id = []
    artist_name = []
    artist_image = []

    for artist in data["artists"]:
        artist_id.append(artist["id"])
        artist_name.append(artist["name"])
        artist_image.append(artist["images"][0]["url"])

    artist_dict = {
        "artist_id": artist_id,
        "artist_name": artist_name, 
        "artist_image": artist_image
    }

    df = pd.DataFrame(artist_dict, columns=artist_dict.keys())
    print(df)
    print(artist_image[0])
    return df


def get_recently_played():
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": ""
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    url = "https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp)
    
    data = request_data(url=url, headers=headers)

    # TODO if listened to at least 5 songs from one playlist back to back = listened to playlist 

    # played_at_table
    timestamps = []
    played_at_unix_list = []

    # Song_table
    song_ids = []
    song_names = []
    song_duration = []
    song_is_explicit = []

    # album_table
    album_id = []
    album_type = []
    album_name = []
    release_date = []
    album_image = []
    album_artist = []

    # song_region
    song_available_in = { "items": [] }

    # album_region
    album_available_in = { "items": [] }

    # artist_table
    artist_id = []
    
    # playlist_table
    playlist_ids = []

    # genre_table
    genre_name = []

    # TODO context = NULL: not in playlist
    # if something in context then probably playlist
    
    
    for song in data["items"]:
        # album
        album = song["track"]["album"]
        album_id.append(album["id"])
        album_type.append(album["album_type"])
        album_name.append(album["name"])
        release_date.append(album["release_date"])
        album_image.append(album["images"][0]["url"])
        album_artist.append(album["artists"][0]["id"])

        # song
        song_ids.append(song["track"]["id"])
        song_names.append(song["track"]["name"])
        song_duration.append(song["track"]["duration_ms"])
        song_is_explicit.append(song["track"]["explicit"])

        # played_at
        unix_time = convert_timestamp(song["played_at"])
        #timestamp = datetime.datetime.fromtimestamp(unix_time)
        timestamp = song["played_at"][0:10]
        played_at_unix_list.append(unix_time)
        timestamps.append(timestamp)
        
        # playlist ids
        if song["context"] is not None:
            playlist_id = song["context"]["uri"].split(":")[2]
            playlist_ids.append(playlist_id)
        
        # artist
        artist = song["track"]["artists"][0]
        artist_id.append(artist["id"])


    song_in_album = {
        "song_id": song_ids,
        "album_id": album_id
    }

    song_dict = {
        "song_id": song_ids,
        "song_name": song_names,
        "duration": song_duration,
        "is_explicit": song_is_explicit
    }

    played_at_dict = {
        "played_at": played_at_unix_list,
        "played_at_date": timestamps,
        "song_id": song_ids
    }

    album_dict = {
        "album_id": album_id,
        "album_type": album_type,
        "album_name": album_name,
        "release_date": release_date,
        "album_image": album_image
    }

    playlist_ids = set(playlist_ids)
    song_df = pd.DataFrame(song_dict, columns=["song_id", "song_name", "duration", "is_explicit"])
    album_df = pd.DataFrame(album_dict, columns=["album_id", "album_type", "album_name", "release_date", "album_image"])
    song_in_album_df = pd.DataFrame(song_in_album, columns=["song_id", "album_id"])
    played_at_dict_df = pd.DataFrame(played_at_dict, columns=["played_at", "played_at_date", "song_id"])

    song_df = song_df.drop_duplicates(["song_id"], keep="first")
    album_df = album_df.drop_duplicates(["album_id"], keep="first")
    song_in_album_df = song_in_album_df.drop_duplicates(["song_id", "album_id"], keep="first")

    write_to_database(song_df, "Song")
    write_to_database(album_df, "Album")
    write_to_database(song_in_album_df, "song_in_album")
    write_to_database(played_at_dict_df, "played_at")
    #song_df.to_sql("Song", engine, index=False, if_exists='append')
    #album_df.to_sql("Album", engine, index=False, if_exists='append')
    #song_in_album_df.to_sql("song_in_album", engine, index=False, if_exists='append')
    #played_at_dict_df.to_sql("played_at", engine, index=False, if_exists='append')

    #song_df = song_df.sort_values(by="timestamp_unix", ascending=True, na_position="first")        


def db_query():
    #compare_data = pd.read_sql(f'SeLeCt {primary_key} fRoM {table};', con=engine)
    pass



create_database()
get_recently_played()
#url = "https://api.spotify.com/v1/tracks/1ZGeKrKrY3JN3iSfmF4T5j"
#url = "https://api.spotify.com/v1/artists/3o2dn2O0FCVsWDFSh8qxgG"
#data = request_data(url)
#artist_list = ["3o2dn2O0FCVsWDFSh8qxgG", "4ZGFYwFpBWxq6FxqilvRJT", "5KWOCu1saEHAhPiLKlOLIy", "35HMF6Y8dSzNfeJs3X65fw", "54NKhABnyGAvbek0n63TAu"]

#data = get_artists(artist_list)

# Blend
#data = request_data("https://api.spotify.com/v1/playlists/37i9dQZF1EJBfSGYDdU6jO")

# My Playlist
#data = request_data("https://api.spotify.com/v1/playlists/2tbHgpS0fMAVVxxwwTdQuf")
#with open("data.json", "w") as outfile:
#    outfile.write(json.dumps(data, indent=4))


#execute_sql_script("database_creation_script.sql")
#write_to_database(get_artists(artist_ids=artist_list),"Artist")

#file = "region_codes.json"
#authorization_file = open(file)
#authentication_data = json.load(authorization_file)
#authentication_data = {"country": authentication_data}

#with open(file, "w") as outfile:
#    outfile.write(json.dumps(authentication_data, indent=4))