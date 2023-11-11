import sqlalchemy, sqlite3, requests, json
import pandas as pd

from main import print_status_message, get_token

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('my_played_tracks.sqlite')
cursor = conn.cursor()


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


def request_data():
    url = "https://api.spotify.com/v1/artists/3o2dn2O0FCVsWDFSh8qxgG"

    TOKEN = get_token()
    headers = {
        #"Accept": "application/json",
        #"Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    #print("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp))
    print_status_message("Testing API")
    r = requests.get(
        url,
        headers=headers)
    if r.status_code == 200:
        return r.json()
    else:
        print_status_message("Artist information could not be fetched")
        # TODO: error handling
        return

      
data = request_data()
with open("data.json", "w") as outfile:
    outfile.write(json.dumps(data, indent=4))
