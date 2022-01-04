""" Database Configuration for TuneCapsule

Copyright (c) 2021 IdmFoundInHim, under MIT License
"""
__all__ = ["initialize_database"]

import os
import sqlite3

from ._constants import DB_DIRECTORY, DB_LOCATION


def initialize_database():
    try:
        os.makedirs(DB_DIRECTORY)
    except FileExistsError:
        pass
    CLASSIFICATION_TABLE_TEMPLATE = """release_day INTEGER,
                              artist_names TEXT,
                              name TEXT,
                              classification TEXT,
                              track_names TEXT,
                              track_durations_sec TEXT,
                              track_numbers TEXT,
                              retrieved_time INTEGER,
                              artist_group TEXT,
                              album_spotify_id TEXT,
                              track_spotify_ids TEXT"""
    with sqlite3.connect(DB_LOCATION) as database:
        database.executescript(
            f"""
        CREATE TABLE ranking (sha256 BLOB PRIMARY KEY,{CLASSIFICATION_TABLE_TEMPLATE});
        CREATE TABLE certification (sha256 BLOB, {CLASSIFICATION_TABLE_TEMPLATE});
        CREATE TABLE season (min_year INTEGER,
                             max_year INTEGER,
                             classification TEXT,
                             start_date INTEGER,
                             stop_date INTEGER,
                             playlist_spotify_id TEXT);
        CREATE TABLE helper_artist_group (artist_group TEXT,
                                          artist_name TEXT,
                                          artist_spotify_id TEXT);
        CREATE TABLE helper_single (single_hash BLOB,
                                    album_hash BLOB);
        CREATE TABLE helper_artist_score (artist_group TEXT,
                                          date_from INTEGER,
                                          score INTEGER)
        """
        )
