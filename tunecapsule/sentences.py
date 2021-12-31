""" TuneCapsule extensions for StreamSort 

Copyright (c) 2021 IdmFoundInHim, under MIT License
"""
__all__ = ["tc_classify", "tc_season"]

import calendar
import itertools
import sqlite3 as sql
from collections.abc import Collection, Iterable, Iterator
from datetime import MAXYEAR, MINYEAR, date, datetime
from hashlib import sha256
from typing import cast

from more_itertools import prepend
from projects import proj_projects
from spotipy import Spotify, SpotifyPKCE
from streamsort import (
    UnexpectedResponseException,
    UnsupportedQueryError,
    results_generator,
    ss_new,
    state_only_api,
    str_mob,
)
from streamsort.types import Mob, Query, State

from ._constants import (
    DB_LOCATION,
    IDEAL_AUTOSEASON_LENGTH,
    MAX_AUTOSEASON,
    RANKINGS,
    SEASON_KEYWORDS,
    SHA256_ENCODING,
    SPOTIFY_DATE_DELIMITER,
)
from .utilities import (
    autoseason_name,
    beginning_year,
    end_year,
    list2strray,
    read_rows,
)

YearRange = tuple[int | None, int | None]
SeasonQueryGroup = tuple[int, int] | int | str
HashDigest = bytes
NULL_YEAR_RANGE = (None, None)


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
    for proj in cast(
        list[Mob], proj_projects(subject, subject.mob).mob["objects"]
    ):
        try:
            classification = cast(str, query).split()[0].upper()
        except AttributeError as err:
            raise UnsupportedQueryError(
                "classify", str_mob(cast(Mob, query))
            ) from err
            # raise UnsupportedQueryError("Classification must be text") from err
        if classification.isnumeric():
            raise UnsupportedQueryError("classify", cast(str, query))
            # raise UnsupportedQueryError("Classification cannot be numeric")
        with sql.connect(DB_LOCATION) as database:
            row = _tc_classify_build_row(
                subject.api, database, classification, proj
            )
            if classification in RANKINGS:
                database.execute(
                    "DELETE FROM ranking WHERE sha256 = ?", (row[0],)
                )
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
            database.execute(
                f"INSERT INTO {target_table} VALUES ({'?, ' * 10}?)", row
            )
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
    classifications. The year can be omitted if the classification is
    not a number to get all songs.

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

    A "season" is, in general, the set of projects having one of the
    given classifications and being released during the given date
    range. Each season's metadata includes the classification(s), date
    range, and a Spotify playlist ID. Metadata is stored in the
    database, while the contents are gathered as needed. Numeric
    classifications (e.g. "1" and "12") represent the set of
    classifications {"A", "B"} and inform this sentence to automatically
    calculate an end and start date.

    Note that *year* ranges are inclusive-inclusive (r[0] <= n <= r[1]) while *day* ranges are inclusive-exclusive (r[0] <= n < r[1], like the `range` builtin)
    """
    db = sql.connect(DB_LOCATION)
    if not isinstance(query, str):
        raise UnsupportedQueryError("season", str_mob(query))
    match _tc_season_parse_query(query):
        case "update", (year, _year) if year == _year:
            out = _tc_season_update_year(subject.api, db, year)
        case "update", (min_year, max_year):
            out = _tc_season_update_years(subject.api, db, min_year, max_year)
        case "update",:
            out = _tc_season_update_years(subject.api, db, *NULL_YEAR_RANGE)
        case "update", (min_year, max_year), str(classification):
            target = _tc_season_retrieve_metadata(
                db, (min_year, max_year), classification
            )
            out = _tc_season_upload(
                subject.api,
                db,
                classification,
                target["start_date"],
                target["end_date"],
                target["playlist_id"],
            )
        case "update", str(classification):
            target = _tc_season_retrieve_metadata(
                db, NULL_YEAR_RANGE, classification
            )
            out = _tc_season_upload(
                subject.api,
                db,
                classification,
                None,
                None,
                target["playlist_id"],
            )
        case (min_year, max_year), int(season_num):
            start_date = _tc_season_calculate_start(db, min_year, season_num)
            end_date = _tc_season_calculate_end(db, start_date, max_year)
            playlist_id = subject.mob["id"]
            out = _tc_season_create(
                subject.api,
                db,
                (min_year, max_year),
                season_num,
                start_date,
                end_date,
                playlist_id,
            )
        case (min_year, max_year), str(classification):
            start_date = beginning_year(min_year)
            end_date = end_year(max_year)
            playlist_id = subject.mob["id"]
            out = _tc_season_create(
                subject.api,
                db,
                (min_year, max_year),
                classification,
                start_date,
                end_date,
                playlist_id,
            )
        case str(classification),:
            playlist_id = subject.mob["id"]
            out = _tc_season_create(
                subject.api,
                db,
                NULL_YEAR_RANGE,
                classification,
                None,
                None,
                playlist_id,
            )
        case _:
            out = None
            raise UnsupportedQueryError("season", query)
    return State(subject[0], out or subject[1], subject[2])


def _tc_classify_build_row(
    api: Spotify, db: sql.Connection, classification: str, project: Mob
) -> tuple[bytes, date, str, str, str, str, str, datetime, str, str, str,]:
    album = cast(Mob, api.album(project["root_album"]["uri"]))
    retrieved_time = datetime.now()

    try:
        match album["release_date"].split(SPOTIFY_DATE_DELIMITER):
            case yr, mo, da:
                release_day = date(int(yr), int(mo), int(da))
            case yr, mo:
                release_day = date(
                    int(yr), int(mo), calendar.monthrange(yr, mo)[1]
                )
            case [yr]:
                release_day = end_year(yr)
            case _:
                raise UnexpectedResponseException
    except (TypeError, ValueError) as err:
        raise UnexpectedResponseException from err
    artist_zip = sorted((a["name"], a["id"]) for a in album["artists"])
    artist_names, artist_group = map(list2strray, zip(*artist_zip))
    _tc_classify_store_artist_group(db, artist_group, artist_zip)
    name = cast(str, project["name"])
    hash_digest = sha256(
        bytes(release_day.isoformat() + artist_names + name, SHA256_ENCODING)
    ).digest()
    included_track_ids = [t["id"] for t in project["objects"]]
    track_names, track_numbers, track_spotify_ids = map(
        list2strray,
        zip(
            *[
                (t["name"], t["track_number"], t["id"])
                for t in results_generator(
                    cast(SpotifyPKCE, api.auth_manager), album["tracks"]
                )
                if t["id"] in included_track_ids
            ]
        ),
    )
    album_spotify_id = cast(str, album["id"])
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


def _tc_season_upload(
    api: Spotify,
    db: sql.Connection,
    classification: str | int,
    start_date: date | None,
    end_date: date | None,
    playlist_id: str,
) -> Mob:
    season = _tc_season_retrieve_projects(
        db, classification, start_date, end_date
    )
    return _tc_season_transmit_projects(api, playlist_id, season)


def _tc_season_create(
    api: Spotify,
    db: sql.Connection,
    year_range: YearRange,
    classification: str | int,
    start_date: date | None,
    end_date: date | None,
    playlist_id: str,
) -> Mob:
    _tc_season_store_metadata(
        db, year_range, classification, start_date, end_date, playlist_id
    )
    return _tc_season_upload(
        api, db, classification, start_date, end_date, playlist_id
    )


def _tc_season_ensure_autoseason(
    api: Spotify,
    db: sql.Connection,
    year_range: tuple[int, int],
    season_number: int,
    season_dates: tuple[date, date],
) -> Mob:
    try:
        playlist = _tc_season_retrieve_metadata(db, year_range, season_number)[
            "playlist_id"
        ]
        return _tc_season_upload(
            api, db, season_number, *season_dates, playlist
        )
    except IndexError:  # Ensure no unintentional capture
        return _tc_season_create(
            api,
            db,
            year_range,
            season_number,
            *season_dates,
            ss_new(
                state_only_api(api), autoseason_name(year_range, season_number)
            ).mob["id"],
        )


def _tc_season_update_year(
    api: Spotify, db: sql.Connection, year: int
) -> Mob | None:
    last_playlist = None
    seasons = itertools.pairwise(_tc_season_calculate_year(db, year))
    for season_number, season_dates in enumerate(seasons):
        last_playlist = _tc_season_ensure_autoseason(
            api, db, (year, year), season_number, season_dates
        )
    return last_playlist


def _tc_season_update_years(
    api: Spotify, db, min_year, max_year
) -> Mob | None:
    last_playlist = None
    # Loop constant:
    max_year = max_year or date.today().year
    # Loop variables:
    total = 0
    target_min = min_year or _tc_season_retrieve_min_year(db)
    target_max = target_min
    while target_max <= max_year:
        # Each run of the loop produces no more than one
        # playlist. As a result, the loop may run more than
        # once with a specific value of `target_max`, but it
        # will never be run with the same pair of
        # `(target_min, target_max)` values. This design choice
        # was made to ease debugging.
        selected_len = _tc_season_retrieve_year_len(db, target_max)
        total += selected_len
        if total >= IDEAL_AUTOSEASON_LENGTH or target_max == max_year:
            if (
                target_min != target_max
                and selected_len >= IDEAL_AUTOSEASON_LENGTH
            ):
                # Stop the range short of ideal length because
                # the next year that could be included is long
                # enough for its own season(s)
                target_max -= 1
            if target_min == target_max:
                last_playlist = _tc_season_update_year(api, db, target_min)
            elif total:
                last_playlist = _tc_season_ensure_autoseason(
                    api,
                    db,
                    (target_min, target_max),
                    1,
                    (beginning_year(target_min), end_year(target_max)),
                )
            total = 0
            target_min = target_max = target_max + 1
        else:
            target_max += 1
    return last_playlist


def _tc_season_calculate_end(
    db: sql.Connection, start_date: date, max_year: int
) -> date:
    """Calculates end date for an ~80 song autoseason"""
    ...


def _tc_season_calculate_start(
    db: sql.Connection, year: int, season_number: int
) -> date:
    """Finds the available start date for an autoseason"""
    ...


def _tc_season_calculate_year(db: sql.Connection, year: int) -> Iterable[date]:
    """Returns sequence of dates for ~80 song autoseasons over a year

    Should be expected to return any type of iterable, anticipating
    later optimization. In particular, this function could be modified
    to look at adjacent seasons, adjusting to minimize total deviation
    from the expected playlist length.

    `len` of return value should not exceed 1 if there are 0 projects in
    the selected year.
    """
    ...


def _tc_season_parse_query(query: str) -> Iterator[SeasonQueryGroup]:
    query_tokens = (_tc_season_parse_token(t) for t in cast(str, query))
    for token in query_tokens:
        if isinstance(token, str) and token not in SEASON_KEYWORDS:
            try:
                yield " ".join(
                    prepend(token, cast(Iterator[str], query_tokens))
                )
            except TypeError:
                raise UnsupportedQueryError("season", query)
        elif token == "update":
            yield "update"
            next(query_tokens)
        else:
            yield token


def _tc_season_parse_token(token: str) -> SeasonQueryGroup:
    match token.split("-"):
        case min, max if len(max) == 2 and len(
            min
        ) == 4 and max.isdigit() and min.isdigit():
            return (int(min), int(min[:2] + max))
        case min, max if len(max) == 4 and len(
            min
        ) == 4 and max.isdigit() and min.isdigit():
            return (int(min), int(max))
        case yr, if yr.isdigit() and len(yr) == 4:
            return cast(tuple[int, int], (int(yr),) * 2)
        case num, if num.isdigit() and len(num) < 4:
            return int(num)
    return token


def _tc_season_retrieve_metadata(
    db: sql.Connection, year_range: YearRange, classification: str | int
) -> Mob:
    """Gets metadata for a single season from the database"""
    ...


def _tc_season_retrieve_min_year(db: sql.Connection) -> int:
    """Finds the earliest release year among ranked tracks"""
    ...


def _tc_season_retrieve_projects(
    db: sql.Connection,
    classification: str | int,
    start_date: date | None,
    end_date: date | None,
) -> Mob:
    """Gathers projects belonging in a single season in release order

    If a date is `None`, the datetime.MAXYEAR and datetime.MINYEAR will
    be used to bound the season.
    """
    ...


def _tc_season_retrieve_year_len(db: sql.Connection, year: int) -> int:
    """Counts the tracks in a year eligible for autoseasons"""
    ...


def _tc_season_store_metadata(
    db: sql.Connection,
    year_range: YearRange,
    classification: str | int,
    start_date: date | None,
    end_date: date | None,
    playlist_id: str,
) -> HashDigest:
    """Saves metadata for a single season in the database"""
    ...


def _tc_season_transmit_projects(
    spotify: Spotify, playlist: str, season: Mob
) -> Mob:
    """Uploads a season's tracks to a Spotify playlist"""
    ...
