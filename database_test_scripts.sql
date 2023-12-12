SELECT song_name, album_name, release_date from Song s 
inner join song_in_album sia on s.song_id = sia.song_id 
inner join Album a on sia.album_id = a.album_id;