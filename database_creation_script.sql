CREATE TABLE IF NOT EXISTS Song(
    song_id VARCHAR(200) NOT NULL PRIMARY KEY,
    song_name VARCHAR(200) NOT NULL,
    duration int,
    is_explicit boolean
);

CREATE TABLE IF NOT EXISTS played_at(
    played_at int NOT NULL PRIMARY KEY,
    song_id VARCHAR(200), 
    FOREIGN KEY(song_id) REFERENCES Song(song_id)
);

CREATE TABLE IF NOT EXISTS Artist(
    artist_id VARCHAR(200) NOT NULL PRIMARY KEY,
    artist_name VARCHAR(200),
    artist_image VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS Genre(
    genre_id int NOT NULL PRIMARY KEY,
    genre_name VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS Album(
    album_id VARCHAR(200) NOT NULL PRIMARY KEY,
    album_name VARCHAR(200) NOT NULL,
    release_date date NOT NULL,
    album_image VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS Playlist(
    playlist_id VARCHAR(200) NOT NULL PRIMARY KEY,
    current_snapshot VARCHAR(1000) NOT NULL,
    FOREIGN KEY (current_snapshot) REFERENCES Playlist_snapshot(snapshot_id)
);

CREATE TABLE IF NOT EXISTS Playlist_snapshot(
    snapshot_id VARCHAR(1000) NOT NULL PRIMARY KEY,
    playlist_name VARCHAR(200) NOT NULL,
    playlist_image VARCHAR(500) NOT NULL,
    is_public boolean,
    playlist_description VARCHAR(1000),
    is_historic boolean,
    change_date date,
    is_collaborative boolean,
    playlist_id VARCHAR(200) NOT NULL,
    FOREIGN KEY (playlist_id) REFERENCES Playlist(playlist_id)
);


CREATE TABLE IF NOT EXISTS artist_genre(
    artist_id VARCHAR(200),
    genre_id VARCHAR(200),
    PRIMARY KEY (artist_id, genre_id),
    FOREIGN KEY (artist_id) REFERENCES Artist(artist_id),
    FOREIGN KEY (genre_id) REFERENCES Genre(genre_id)
);

CREATE TABLE IF NOT EXISTS song_artist(
    song_id VARCHAR(200),
    artist_id VARCHAR(200),
    PRIMARY KEY (song_id, artist_id),
    FOREIGN KEY (song_id) REFERENCES Song(song_id),
    FOREIGN KEY (artist_id) REFERENCES Artist(artist_id)
);

CREATE TABLE IF NOT EXISTS song_in_album(
    song_id VARCHAR(200),
    album_id VARCHAR(200),
    PRIMARY KEY (song_id, album_id),
    FOREIGN KEY (song_id) REFERENCES Song(song_id),
    FOREIGN KEY (album_id) REFERENCES Album(album_id)
);

CREATE TABLE IF NOT EXISTS song_in_playlist(
    song_id VARCHAR(200),
    playlist_id VARCHAR(200),
    PRIMARY KEY (song_id, playlist_id),
    FOREIGN KEY (song_id) REFERENCES Song(song_id),
    FOREIGN KEY (playlist_id) REFERENCES Playlist(playlist_id)
);
