import sqlalchemy
import sqlite3
import pandas as pd


DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"


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