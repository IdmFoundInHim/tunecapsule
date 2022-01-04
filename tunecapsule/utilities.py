""" Utility functions for TuneCapsule

Copyright (c) 2021 IdmFoundInHim, under MIT License
"""
from collections.abc import Callable, Collection, Iterable, Iterator
from datetime import date, datetime, timedelta
import sqlite3
from typing import cast

from ._constants import DB_STRRAY_DELIMITER


def _identity(x):
    return x


def strray2list(strray: str) -> list:
    return strray.split(DB_STRRAY_DELIMITER)


def list2strray(lst: Iterable) -> str:
    return DB_STRRAY_DELIMITER.join(map(str, lst))


DB_COLUMNS: dict[str, Callable] = {
    "sha256": bytes,
    "release_day": date.fromisoformat,
    "artist_names": strray2list,
    "name": str,
    "classification": str,
    "track_names": strray2list,
    "track_durations_sec": lambda s: [
        timedelta(seconds=int(n)) for n in strray2list(s)
    ],
    "track_numbers": lambda s: [int(n) for n in strray2list(s)],
    "retrieved_time": datetime.fromisoformat,
    "artist_group": strray2list,
    "album_spotify_id": str,
    "track_spotify_ids": strray2list,
    "year_range": lambda x: x.split("-"),
    "start_date": date.fromisoformat,
    "stop_date": date.fromisoformat,
    "playlist_spotify_id": str,
    "artist_name": str,
    "artist_spotify_id": str,
}


def read_rows(cursor: sqlite3.Cursor, columns: str) -> Iterator[tuple]:
    parsers = [
        # Matches SQL format for column names, ignoring table names
        DB_COLUMNS[name.strip().split(".")[-1]]
        for name in columns.split(",")
    ]
    while row := cursor.fetchone():
        yield tuple(parser(column) for column, parser in zip(row, parsers))


def beginning_year(year: int):
    return date(year, 1, 1)


def end_year(year: int):
    return date(year, 12, 31)


def autoseason_name(year_range: tuple[int, int], season_number: int):
    if year_range[0] == year_range[1]:
        return f"{year_range[0]} {season_number}"
    else:
        return f"{year_range[0]}-{year_range[1]} {season_number}"


def sql_array(options: Collection):
    """Puts in placeholders for an expanded collection in SQL params"""
    return "(" + ", ".join(("?",) * len(options)) + ")"
