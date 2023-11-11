import sqlalchemy, sqlite3, requests, json
import pandas as pd
import datetime

from main import print_status_message, get_token

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('my_played_tracks.sqlite')
CURSOR = conn.cursor()


# Creating Databases (hardcoded according to the Entity Relationship Model)
def create_databases():
    sql_query = """
    CREATE TABLE IF NOT EXISTS Song(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        timestamp_unix VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
    pass


def write_to_database(song_df):
    #print(song_df)
    

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

    CURSOR.execute(sql_query)
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
    #print("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp))
    print_status_message("Testing API")
    r = requests.get(
        url,
        headers=headers)
    if r.status_code <= 202:
        print(r.status_code)
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
    return request_data(url=url, headers=headers)



get_recently_played()
#url = "https://api.spotify.com/v1/tracks/1ZGeKrKrY3JN3iSfmF4T5j"
url = "https://api.spotify.com/v1/artists/3o2dn2O0FCVsWDFSh8qxgG"
#data = request_data(url)
artist_list = ["3o2dn2O0FCVsWDFSh8qxgG", "4ZGFYwFpBWxq6FxqilvRJT", "5KWOCu1saEHAhPiLKlOLIy"]

#data = get_artists(artist_list)

# Blend
data = request_data("https://api.spotify.com/v1/playlists/37i9dQZF1EJBfSGYDdU6jO")

# My Playlist
#data = request_data("https://api.spotify.com/v1/playlists/2tbHgpS0fMAVVxxwwTdQuf")
with open("data.json", "w") as outfile:
    outfile.write(json.dumps(data, indent=4))

