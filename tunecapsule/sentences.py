""" TuneCapsule extensions for StreamSort 

Copyright (c) 2021 IdmFoundInHim, under MIT License
"""
__all__ = ["tc_classify", "tc_season"]

import calendar
import itertools
import os
import sqlite3 as sql
from collections.abc import Iterable, Sequence
from datetime import date, datetime, timedelta
from hashlib import sha256
from typing import Collection, cast

import more_itertools
from more_itertools import grouper
from projects import proj_projects
from spotipy import Spotify
from streamsort import (
    UnexpectedResponseException,
    UnsupportedQueryError,
    results_generator,
    ss_add,
    ss_open,
    ss_remove,
    str_mob,
)
from streamsort.types import Mob, Query, State

from ._constants import DB_LOCATION, RANKINGS, SHA256_ENCODING, SPOTIFY_DATE_DELIMITER
from .utilities import list2strray, read_rows


def tc_classify(subject: State, query: Query) -> State:
    """Associates a project(s) with a classification

    The subject will be assigned the query as a classification.
    Classifications can either be rankings or certifications. Rankings
    are A, B, C, and E. Certifications are other strings except for
    numbers.

    When adding a ranking of a project with at least five songs,
    previous rankings will be searched for singles that appear on the
    newly-ranked project. Lower rankings (e.g. "C" single now appearing
    on "B" project) will be removed, but higher rankings (e.g. songs
    from a short "A" album now appearing on a longer "B" album) will
    remain.

    The database preserves both a static representation
    of each project based on its state in Spotify at runtime and a
    dynamic representation of each project by Spotify URIs.
    """
    for proj in cast(list[Mob], proj_projects(subject, subject.mob).mob["objects"]):
        try:
            classification = query.split()[0].upper()
        except TypeError as err:
            raise UnsupportedQueryError("Classification must be text") from err
        if classification.isnumeric():
            raise UnsupportedQueryError("Classification cannot be numeric")
        with sql.connect(DB_LOCATION) as database:
            row = _tc_classify_build_row(subject.api, database, classification, proj)
            if classification in RANKINGS:
                database.execute("DELETE FROM ranking WHERE sha256 = ?", (row[0],))
                target_table = "ranking"
                # TODO Add processing of singles
            else:
                columns_select = "classification, track_names"
                existing_rows = database.execute(
                    f"SELECT {columns_select} FROM certification WHERE sha256 = ?",
                    (row[0],),
                ).fetchall()
                if any(ex == row[4:6] for ex in existing_rows):
                    # TODO Warn about duplicate
                    return subject
                target_table = "certification"
            database.execute(f"INSERT INTO {target_table} VALUES ({'?, ' * 10}?)", row)
    return subject


def tc_season(subject: State, query: Query) -> State:
    """Associates a playlist with certain rankings or a certification

    The subject will be **overwritten** to hold songs in chronological
    order of release holding a minimum ranking or given certification.
    The query describes a time range and certification. An optional
    first word, "update", changes this behavior to modify playlists that
    have been previously assigned.
    
    The query includes a year (e.g. 2019) or year range (e.g.
    1999-2019), and then all subsequent words are classifications.If the
    classification is a number, the playlist is used to hold a part of
    the year's songs ranked A or B. Otherwise, the playlist holds all of
    the year's (or range's) songs with at least one of the
    classifications.

    For example, "season 2020 3" would hold about 80 songs from the
    middle of 2020 that were ranked A or B. Meanwhile, "season 2020 B"
    would hold however many songs were ranked B from 2020, but none
    ranked A. "season 2020 C 🔂" would include C-ranked songs from 2020
    as well as 🔂-certified songs from that year.

    The location of the playlist on Spotify will also be saved in the
    database alongside the selected classifications. This allows later
    use of "season update" to add any newly-classified projects to the
    playlist. For instance, if more projects were ranked in 2020 after
    making a 2020-3 playlist, running "season update 2020 3" would add
    those projects to the existing playlist (keeping release order)
    until the playlist is about 80 songs. Alternatively, "season update
    2020" would edit all the previously created 2020 numbered playlists
    (like 2020-1 or 2020-12), potentially moving projects between
    playlists to sort them by release order.

    Broad uses of "season update" or "season update *year*" will create
    playlists with predictable names (e.g. "2010--2017" or "2014-3") to
    fill gaps in the sequence. They will not, however, change the names
    of existing playlists. Only the unlimited "season update" will
    automatically create and remove year ranges (which will not delete
    the playlists on Spotify). Ranges of more than one year must
    encompass only whole years.
    """
    ...


def _tc_classify_build_row(
    api: Spotify, db: sql.Connection, classification: str, project: Mob
) -> tuple[
    bytes, int, str, str, str, list[str], list[int], datetime, str, str, list[str]
]:
    album = api.album(project["root_album"]["uri"])
    retrieved_time = datetime.now()

    try:
        match album["release_date"].split(SPOTIFY_DATE_DELIMITER):
            case yr, mo, da:
                release_day = date(int(yr), int(mo), int(da))
            case yr, mo:
                release_day = date(int(yr), int(mo), calendar.monthrange(yr, mo)[1])
            case [yr]:
                release_day = date(int(yr), 12, 31)
            case _:
                raise UnexpectedResponseException
    except (TypeError, ValueError) as err:
        raise UnexpectedResponseException from err
    artist_zip = sorted((a["name"], a["id"]) for a in album["artists"])
    artist_names, artist_group = map(list2strray, zip(*artist_zip))
    _tc_classify_store_artist_group(db, artist_group, artist_zip)
    name = project["name"]
    hash_digest = sha256(
        bytes(release_day.isoformat() + artist_names + name, SHA256_ENCODING)
    ).digest()
    included_track_ids = [t["id"] for t in project["objects"]]
    track_names, track_numbers, track_spotify_ids = map(
        list2strray,
        zip(
            *[
                (t["name"], t["track_number"], t["id"])
                for t in results_generator(api, album["tracks"])
                if t["id"] in included_track_ids
            ]
        ),
    )
    album_spotify_id = album["id"]
    return (
        hash_digest,
        release_day,
        artist_names,
        name,
        classification,
        track_names,
        track_numbers,
        retrieved_time,
        artist_group,
        album_spotify_id,
        track_spotify_ids,
    )


def _tc_classify_store_artist_group(
    db: sql.Connection, artist_group: str, artists: Collection[tuple[str, str]]
):
    existing_rows = db.execute(
        "SELECT artist_name, artist_spotify_id FROM helper_artist_group WHERE artist_group = ?",
        (artist_group,),
    ).fetchall()
    if not existing_rows:
        for artist in artists:
            db.execute(
                "INSERT INTO helper_artist_group VALUES (?, ?, ?)",
                (artist_group, *artist),
            )
        return
    assert set(existing_rows) == set(artists)
