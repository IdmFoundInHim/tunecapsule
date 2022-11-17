""" Database Configuration for TuneCapsule

Copyright (c) 2021 IdmFoundInHim, under MIT License
"""
__all__ = ["initialize_database"]

import os
import sqlite3

from ._constants import DB_DIRECTORY, DB_LOCATION


def initialize_database():
    try:
        if DB_DIRECTORY:
            os.makedirs(DB_DIRECTORY, exist_ok=True)
    except FileExistsError:
        pass
    CLASSIFICATION_TABLE_TEMPLATE = """
        release_day INTEGER,
        artist_names TEXT,
        name TEXT,
        classification TEXT,
        track_names TEXT,
        track_durations_sec TEXT,
        track_numbers TEXT,
        retrieved_time INTEGER,
        artist_group TEXT,
        album_spotify_id TEXT,
        track_spotify_ids TEXT
    """
    with sqlite3.connect(DB_LOCATION) as database:
        database.executescript(
            f"""
        CREATE TABLE ranking ({CLASSIFICATION_TABLE_TEMPLATE},
            PRIMARY KEY (release_day, artist_names, name)
        );
        CREATE TABLE certification ({CLASSIFICATION_TABLE_TEMPLATE},
            PRIMARY KEY (release_day, artist_names, name, classification)
        );
        CREATE TABLE season (
            min_year INTEGER,
            max_year INTEGER,
            classification TEXT,
            start_date INTEGER,
            stop_date INTEGER,
            playlist_spotify_id TEXT,
            PRIMARY KEY (min_year, max_year, classification)
                ON CONFLICT REPLACE
        );
        CREATE TABLE helper_artist_group (
            artist_group TEXT,
            artist_name TEXT,
            artist_spotify_id TEXT,
            PRIMARY KEY (artist_group, artist_spotify_id) ON CONFLICT IGNORE
        );
        CREATE TABLE helper_single (
            single_release_day INTEGER,
            artist_names TEXT,
            single_name TEXT,
            album_release_day INTEGER,
            album_name TEXT,
            single_track_names TEXT,
            album_track_names TEXT,
            PRIMARY KEY (
                single_release_day,
                artist_names,
                single_name,
                single_track_names
            ) ON CONFLICT REPLACE
        );
        CREATE TABLE helper_artist_score (
            artist_group TEXT,
            date_from INTEGER,
            score INTEGER,
            PRIMARY KEY(artist_group, date_from) ON CONFLICT REPLACE
        );
        """
        )
        database.commit()
