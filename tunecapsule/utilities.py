""" Utility functions for TuneCapsule

Copyright (c) 2021 IdmFoundInHim, under MIT License
"""
from collections.abc import Collection
from datetime import date, datetime

from ._constants import DB_STRRAY_DELIMITER


def _identity(x):
    return x


def strray2list(strray: str) -> list:
    return strray.split(DB_STRRAY_DELIMITER)


def list2strray(lst: list) -> str:
    return DB_STRRAY_DELIMITER.join(map(str, lst))


DB_COLUMNS = {
    "sha256": _identity,
    "release_day": date.fromisoformat,
    "artist_names": strray2list,
    "name": _identity,
    "classification": _identity,
    "track_names": strray2list,
    "track_numbers": lambda s: [int(n) for n in strray2list(s)],
    "retrieved_time": datetime.fromisoformat,
    "artist_group": strray2list,
    "album_spotify_id": _identity,
    "track_spotify_ids": strray2list,
    "year_range": lambda x: x.split("-"),
    "start_date": date.fromisoformat,
    "stop_date": date.fromisoformat,
    "playlist_spotify_id": _identity,
    "artist_name": _identity,
    "artist_spotify_id": _identity,
}


def read_rows(rows: Collection[tuple], columns: str) -> tuple:
    parsers = [DB_COLUMNS[name.strip()] for name in columns.split(",")]
    return [
        tuple(parser(column) for column, parser in zip(row, parsers))
        for row in rows
    ]
