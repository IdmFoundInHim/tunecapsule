"""TuneCapsule extensions for StreamSort

Copyright (c) 2021 IdmFoundInHim, under MIT License
"""

__all__ = ["ss_classify", "ss_season", "ss_score"]

import calendar
import sqlite3 as sql
from collections.abc import Collection, Iterable, Iterator
from datetime import date, datetime, timedelta
from itertools import pairwise
from typing import Sequence, cast

from more_itertools import chunked, flatten, only, prepend
from projects import ss_projects
from spotipy import Spotify, SpotifyPKCE
from streamsort import (
    IO_CONFIRM as io_confirm,
    IO_NOTIFY as io_notify,
    NoResultsError,
    UnexpectedResponseException,
    UnsupportedQueryError,
    results_generator,
    ss_new,
    ss_remove,
    state_only_api,
    str_mob,
    ss_open,
)
from streamsort.types import Mob, Query, State

from ._constants import (
    AUTOSEASON_RANKINGS,
    DB_LOCATION,
    EXCLUSION_CERTIFICATIONS,
    IDEAL_AUTOSEASON_LENGTH,
    RANKINGS,
    SEASON_KEYWORDS,
    SPOTIFY_DATE_DELIMITER,
)
from .stats import store_artist_group_score, overall_artist_score
from .utilities import (
    autoseason_name,
    beginning_year,
    end_year,
    list2strray,
    read_rows,
    sql_array,
    strray2list,
)

YearRange = tuple[int | None, int | None]
SeasonQueryGroup = tuple[int, int] | int | str
HashDigest = bytes
NULL_YEAR_RANGE = (None, None)


def ss_classify(subject: State, query: Query) -> State:
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
    for project in cast(
        list[Mob], ss_projects(subject, subject.mob).mob["objects"]
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
        try:
            with sql.connect(DB_LOCATION) as database:
                _classify_project(
                    subject.api, database, project, classification
                )
        except UnexpectedResponseException as err:
            io_notify(f"Unexpected Response")
            for line in err.args:
                io_notify(f"    {line}")
            io_notify(f'Could not classify "{project['name']}"')
    return subject


def ss_season(subject: State, query: Query) -> State:
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
    ranked A. "season 2020 C MASTERPIECE" would include C-ranked songs from 2020
    as well as "Masterpiece"-certified songs from that year.

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
    match list(_season_parse_query(query)):
        case "update", (year, _year) if year == _year:
            out = _season_update_year(subject.api, db, year)
        case "update", (min_year, max_year):
            out = _season_update_years(subject.api, db, min_year, max_year)
        case "update",:  # TODO seems broken
            out = _season_update_years(subject.api, db, *NULL_YEAR_RANGE)
        case "update", (min_year, max_year), classification if isinstance(
            classification, int | str
        ):
            target = _season_retrieve_metadata(
                db, (min_year, max_year), classification
            )
            out = _season_upload(
                subject.api,
                db,
                list2strray(classification.split()),
                cast(date, target["start_date"]),
                cast(date, target["stop_date"]),
                cast(str, target["playlist_spotify_id"]),
            )
        case "update", str(classification):
            target = _season_retrieve_metadata(
                db, NULL_YEAR_RANGE, classification
            )
            out = _season_upload(
                subject.api,
                db,
                list2strray(classification.split()),
                None,
                None,
                cast(str, target["playlist_spotify_id"]),
            )
        case (min_year, max_year), int(season_num):
            start_date = _season_calculate_start(db, min_year, season_num)
            end_date = _season_calculate_end(db, start_date, max_year)
            playlist_id = subject.mob["id"]
            out = _season_create(
                subject.api,
                db,
                season_num,
                start_date,
                end_date,
                playlist_id,
            )
        case (min_year, max_year), str(classification):
            start_date = beginning_year(min_year)
            end_date = beginning_year(max_year + 1)
            playlist_id = subject.mob["id"]
            out = _season_create(
                subject.api,
                db,
                list2strray(classification.split()),
                start_date,
                end_date,
                playlist_id,
            )
        case str(classification),:
            playlist_id = subject.mob["id"]
            out = _season_create(
                subject.api,
                db,
                list2strray(classification.split()),
                None,
                None,
                playlist_id,
            )
        case _:
            out = None
            raise UnsupportedQueryError("season", query)
    db.commit()
    db.close()
    return State(subject[0], out or subject[1], subject[2])


def ss_score(subject: State, query: Query) -> State:
    """Retrieves classifications for a projectss

    Work in progress. Currently, supports overall artist scores and album rankings.
    """
    if query:
        mob = ss_open(subject, query).mob
    else:
        mob = subject.mob
    database = sql.connect(DB_LOCATION)
    match mob["type"]:
        case "artist":
            io_notify(overall_artist_score(database, mob["id"], None))
        case "album":
            rows = database.execute(
                "SELECT classification FROM ranking WHERE album_spotify_id = ?",
                (mob["id"],),
            )
            try:
                io_notify(rows.fetchone()[0])
            except TypeError as err:
                raise NoResultsError from err
    database.close()
    return State(subject[0], mob, subject[2])


def _classify_project(
    api: Spotify, db: sql.Connection, proj: Mob, classification: str
):
    if proj["root_album"]["uri"] is None:
        return
    row = _classify_build_row(api, db, classification, proj)
    if classification in RANKINGS:
        row = _classify_rank(db, row)
        target_table = "ranking"
    else:
        row = _classify_certify(api, db, row)
        target_table = "certification"
    if not row:
        return
    db.execute(
        f"INSERT INTO {target_table} VALUES {sql_array(row)}",
        (
            *row[0:4],
            *map(list2strray, (row[4], [d.seconds for d in row[5]], row[6])),
            *row[7:10],
            list2strray(row[10]),
        ),
    )


def _classify_rank(db, row):
    columns = "release_day, artist_names, name, classification, track_names, track_durations_sec, track_numbers, retrieved_time, artist_group, album_spotify_id, track_spotify_ids"
    existing_row = only(
        read_rows(
            db.execute(
                f"""
            SELECT {columns} FROM ranking
            WHERE release_day = ? AND artist_names = ? AND name = ?
            """,
                row[0:3],
            ),
            columns,
        )
    )
    if (
        existing_row
        and existing_row[9] == row[9]
        and existing_row[3] != row[3]
        and len(existing_row[5]) - len(row[5]) < 4
    ):
        # Existing album (Spotify) ID matches and different ranking
        # Also not removing a destructive # of songs
        io_notify(f"*{row[2]}* by {row[1]} is being re-ranked")
        db.execute(
            """
            DELETE FROM ranking
            WHERE release_day = ? AND artist_names = ? AND name = ?
            """,
            row[0:3],
        )
    elif existing_row and existing_row[9] != row[9]:
        # Album Spotify ID did not match
        io_notify(f"*{row[2]}* by {row[1]} caused a conflict and was skipped")
        return
    elif existing_row:
        return
    return _classify_single_check(db, row)


def _classify_certify(api, db, row):
    columns = "release_day, artist_names, name, classification, track_names, track_durations_sec, track_numbers, retrieved_time, artist_group, album_spotify_id, track_spotify_ids"
    existing_row = only(
        read_rows(
            db.execute(
                f"""
            SELECT {columns} FROM certification
            WHERE release_day = ? AND artist_names = ? AND name = ?
                AND classification = ?
            """,
                row[0:4],
            ),
            columns,
        )
    )
    if existing_row and existing_row[9] == row[9]:
        for column in range(4, 7):
            existing_row[column].append(row[column])
        all_track_ids = existing_row[10] + row[10]
        db.execute(
            """
            DELETE FROM certification
            WHERE release_day = ? AND artist_names = ? AND name = ?
                AND classification = ?
            """,
            row[0:4],
        )
        row = _classify_build_row(
            api,
            db,
            row[3],
            Mob(
                {
                    "name": row[2],
                    "root_album": {"uri": f"spotify:album:{row[9]}"},
                    "objects": [{"id": id} for id in all_track_ids],
                }
            ),
        )
    elif existing_row:
        io_notify(f"*{row[2]}* by {row[1]} caused a conflict and was skipped")
        return
    return row


def _classify_single_check(db, row):
    columns = "release_day, artist_names, name, classification, track_names, track_durations_sec"
    existing_projects = read_rows(
        db.execute(
            f"""
        SELECT {columns} FROM ranking INNER JOIN helper_artist_group
            ON helper_artist_group.artist_group = ranking.artist_group
                AND helper_artist_group.artist_spotify_id = ?
        """,
            (strray2list(row[8])[0],),
        ),
        columns,
    )
    new_classification = row[3]
    new_names, new_durations = row[4:6]
    for (
        ex_release,
        ex_artists,
        ex_name,
        ex_classification,
        ex_names,
        ex_durations,
    ) in existing_projects:
        if _classify_is_single(
            ex_names, ex_durations, new_names, new_durations
        ):
            # Existing is single of new
            if new_classification in RANKINGS and RANKINGS.index(
                new_classification
            ) >= RANKINGS.index(ex_classification):
                _classify_delete_single(db, ex_release, ex_artists, ex_name)
                continue
            _classify_store_single(
                db,
                single_release_day=ex_release,
                artist_names=list2strray(ex_artists),
                single_name=ex_name,
                album_release_day=row[0],
                album_name=row[2],
                single_track_names=ex_names,
                album_track_names=new_names,
            )
        elif _classify_is_single(
            new_names, new_durations, ex_names, ex_durations
        ):
            # New is single of existing
            if new_classification in RANKINGS and RANKINGS.index(
                ex_classification
            ) >= RANKINGS.index(new_classification):
                return
            _classify_store_single(
                db,
                *row[:3],
                album_release_day=ex_release,
                album_name=ex_name,
                single_track_names=new_names,
                album_track_names=ex_names,
            )
        else:
            continue
    return row


def _classify_store_single(
    database,
    single_release_day: date,
    artist_names: str,
    single_name: str,
    album_release_day: date,
    album_name: str,
    single_track_names: list[str],
    album_track_names: list[str],
):
    database.execute(
        f"INSERT INTO helper_single VALUES {sql_array(range(7))}",
        (
            single_release_day,
            artist_names,
            single_name,
            album_release_day,
            album_name,
            list2strray(single_track_names),
            list2strray(album_track_names),
        ),
    )


def _classify_delete_single(database, release, artist_names, name):
    database.execute(
        "DELETE FROM ranking WHERE release_day = ? AND artist_names = ? AND name = ?",
        (release, list2strray(artist_names), name),
    )


def _classify_is_single(
    single_names: Sequence[str],
    single_durations: Sequence[timedelta],
    album_names: Sequence[str],
    album_durations: Sequence[timedelta],
) -> bool:
    for name in single_names:
        if (
            name not in album_names
            or abs(
                (
                    album_durations[album_names.index(name)].seconds
                    - single_durations[single_names.index(name)].seconds
                )
            )
            > 5
        ):
            break
    else:
        return True
    return False


def _classify_build_row(
    api: Spotify, db: sql.Connection, classification: str, project: Mob
) -> tuple[
    date,
    str,
    str,
    str,
    list[str],
    list[timedelta],
    list[int],
    datetime,
    str,
    str,
    list[str],
]:
    album = cast(Mob, api.album(project["root_album"]["uri"]))
    retrieved_time = datetime.now()

    try:
        release_day = _classify_parse_release(
            album["release_date"].split(SPOTIFY_DATE_DELIMITER)
        )
    except AttributeError as err:
        release_day = date.max  # Should not go in database
        raise UnexpectedResponseException from err
    artist_zip = sorted((a["name"], a["id"]) for a in album["artists"])
    artist_names, artist_group = map(list2strray, zip(*artist_zip))
    _classify_store_artist_group(db, artist_group, artist_zip)
    name = cast(str, album["name"])
    included_track_ids = [t["id"] for t in project["objects"]]
    tracks = [
        (
            t["name"],
            t["duration_ms"],
            t["track_number"],
            t["id"],
        )
        for t in results_generator(
            cast(SpotifyPKCE, api.auth_manager), album["tracks"]
        )
        if t["id"] in included_track_ids
    ]
    track_names, track_durations_sec, track_numbers, track_spotify_ids = (
        [],
        [],
        [],
        [],
    )
    for track in tracks:
        track_names.append(str(track[0]))
        track_durations_sec.append(timedelta(milliseconds=track[1]))
        track_numbers.append(int(track[2]))
        track_spotify_ids.append(str(track[3]))
    album_spotify_id = cast(str, album["id"])
    return (
        release_day,
        artist_names,
        name,
        classification,
        track_names,
        track_durations_sec,
        track_numbers,
        retrieved_time,
        artist_group,
        album_spotify_id,
        track_spotify_ids,
    )


def _classify_parse_release(release_date):
    try:
        match release_date:
            case yr, mo, da:
                release_day = date(int(yr), int(mo), int(da))
            case yr, mo:
                release_day = date(
                    int(yr), int(mo), calendar.monthrange(int(yr), int(mo))[1]
                )
            case yr,:
                release_day = end_year(int(yr))
            case _:
                raise UnexpectedResponseException
    except (TypeError, ValueError) as err:
        raise UnexpectedResponseException from err
    return release_day


def _classify_store_artist_group(
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
    # The rest is conflict resolution
    if set(existing_rows) != set(artists):
        if len(existing_rows) == len(artists):
            UnexpectedResponseException("Artist name has changed")
        old = {t[1]: t[0] for t in existing_rows}
        new = {t[1]: t[0] for t in artists}
        for artist_spotify_id in old:
            if old[artist_spotify_id] != new[artist_spotify_id]:
                if io_confirm(
                    f"Rename {old[artist_spotify_id]} to {new[artist_spotify_id]}? "
                ):
                    db.execute(
                        "UPDATE ranking SET artist_names = replace(artist_names, ?, ?) WHERE artist_names LIKE '%' || ? || '%'",
                        (
                            old[artist_spotify_id],
                            new[artist_spotify_id],
                            old[artist_spotify_id],
                        ),
                    )
                    db.execute(
                        "UPDATE certification SET artist_names = replace(artist_names, ?, ?) WHERE artist_names LIKE '%' || ? || '%'",
                        (
                            old[artist_spotify_id],
                            new[artist_spotify_id],
                            old[artist_spotify_id],
                        ),
                    )
                    db.execute(
                        "UPDATE helper_artist_group SET artist_name = ? WHERE artist_spotify_id = ?",
                        (new[artist_spotify_id], artist_spotify_id),
                    )
                    io_notify("Done")
                else:
                    raise UnexpectedResponseException(
                        "Artist name has changed"
                    )


def _season_upload(
    api: Spotify,
    db: sql.Connection,
    classification: str | int,
    start_date: date | None,
    stop_date: date | None,
    playlist_id: str,
) -> Mob:
    for artist_group, release_day in _season_retrieve_rows(
        db,
        "{0}.artist_group, {0}.release_day",
        classification,
        start_date,
        stop_date,
        _season_verify_exclusions(classification),
    ):
        store_artist_group_score(db, artist_group, release_day)
    season = _season_retrieve_tracks(db, classification, start_date, stop_date)
    return_value = _season_transmit_projects(api, playlist_id, season)
    db.commit()
    return return_value


def _season_create(
    api: Spotify,
    db: sql.Connection,
    classification: str | int,
    start_date: date | None,
    end_date: date | None,
    playlist_id: str,
) -> Mob:
    _season_store_metadata(
        db, classification, start_date, end_date, playlist_id
    )
    return _season_upload(
        api, db, classification, start_date, end_date, playlist_id
    )


def _season_ensure_autoseason(
    api: Spotify,
    db: sql.Connection,
    year_range: tuple[int, int],
    season_number: int,
    season_dates: tuple[date, date],
) -> Mob:
    try:
        playlist = _season_retrieve_metadata(db, year_range, season_number)[
            "playlist_spotify_id"
        ]
        return _season_upload(
            api, db, season_number, *season_dates, cast(str, playlist)
        )
    except NoResultsError:  # Ensure no unintentional capture
        return _season_create(
            api,
            db,
            season_number,
            *season_dates,
            ss_new(
                state_only_api(api), autoseason_name(year_range, season_number)
            ).mob["id"],
        )


def _season_update_year(
    api: Spotify, db: sql.Connection, year: int
) -> Mob | None:
    last_playlist = None
    seasons = pairwise(_season_calculate_year(db, year))
    for season_number, season_dates in enumerate(seasons, 1):
        last_playlist = _season_ensure_autoseason(
            api, db, (year, year), season_number, season_dates
        )
    return last_playlist


def _season_update_years(
    api: Spotify,
    db: sql.Connection,
    min_year: int | None,
    max_year: int | None,
) -> Mob | None:
    last_playlist = None
    # Loop constant:
    max_year = max_year or date.today().year
    # Loop variables:
    total = 0
    target_min = min_year or _season_retrieve_min_year(db)
    target_max = target_min
    while target_max <= max_year:
        # Each run of the loop produces no more than one
        # playlist. As a result, the loop may run more than
        # once with a specific value of `target_max`, but it
        # will never be run with the same pair of
        # `(target_min, target_max)` values. This design choice
        # was made to ease debugging.
        selected_len = _season_retrieve_year_len(db, target_max)
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
                last_playlist = _season_update_year(api, db, target_min)
            elif total:
                last_playlist = _season_ensure_autoseason(
                    api,
                    db,
                    (target_min, target_max),
                    1,
                    (
                        beginning_year(target_min),
                        beginning_year(target_max + 1),
                    ),
                )
            total = 0
            target_min = target_max = target_max + 1
        else:
            target_max += 1
    return last_playlist


def _season_calculate_end(
    db: sql.Connection, start_date: date, max_year: int
) -> date:
    """Calculates end date for an ~80 song autoseason"""
    stop_date = stop_day_max = beginning_year(max_year + 1)
    table = _season_retrieve_rows(
        db,
        "ranking.track_names, ranking.release_day",
        0,
        start_date,
        stop_date,
        EXCLUSION_CERTIFICATIONS,
    )
    total_tracks, day, day_tracks = 0, "", 0
    for track_names, release_day in table:
        project_tracks = len(track_names)
        total_tracks += project_tracks
        if release_day != day:
            day, day_tracks = release_day, project_tracks
        else:
            day_tracks += project_tracks
        if total_tracks >= 80:
            # Adjust one day to minimize absolute deviation from ideal
            if (
                IDEAL_AUTOSEASON_LENGTH - (total_tracks - day_tracks)
                < total_tracks - IDEAL_AUTOSEASON_LENGTH
            ):
                stop_date = release_day
            else:
                stop_date = release_day + timedelta(1)
            break
    return stop_date


def _season_calculate_start(
    db: sql.Connection, year: int, season_number: int
) -> date:
    """Finds the available start date for an autoseason"""
    row = db.execute(
        "SELECT stop_date FROM season WHERE min_year = ? AND max_year = ? AND classification < ? ORDER BY stop_date DESC LIMIT 1",
        (year, year, season_number),
    ).fetchone()
    if row:
        return date.fromisoformat(row[0])
    return beginning_year(year)


def _season_calculate_year(db: sql.Connection, year: int) -> Iterable[date]:
    """Returns sequence of dates for ~80 song autoseasons over a year

    Should be expected to return any type of iterable, anticipating
    later optimization. In particular, this function could be modified
    to look at adjacent seasons, adjusting to minimize total deviation
    from the expected playlist length.

    `len` of return value should not exceed 1 if there are 0 projects in
    the selected year.
    """
    if not _season_retrieve_year_len(db, year):
        return []
    season_divider = beginning_year(year)
    yield season_divider
    while season_divider != beginning_year(year + 1):
        season_divider = _season_calculate_end(db, season_divider, year)
        yield season_divider


def _season_parse_query(query: str) -> Iterator[SeasonQueryGroup]:
    query_tokens = (_season_parse_token(t) for t in cast(str, query.split()))
    for token in query_tokens:
        if isinstance(token, str) and token not in SEASON_KEYWORDS:
            try:
                yield " ".join(
                    prepend(token, cast(Iterator[str], query_tokens))
                ).upper()
            except TypeError as err:
                raise UnsupportedQueryError("season", query) from err
        else:
            yield token


def _season_parse_token(token: str) -> SeasonQueryGroup:
    match token.split("-"):
        case min, max if (
            len(max) == 2 and len(min) == 4 and max.isdigit() and min.isdigit()
        ):
            return (int(min), int(min[:2] + max))
        case min, max if (
            len(max) == 4 and len(min) == 4 and max.isdigit() and min.isdigit()
        ):
            return (int(min), int(max))
        case yr, if yr.isdigit() and len(yr) == 4:
            return cast(tuple[int, int], (int(yr),) * 2)
        case num, if num.isdigit() and len(num) < 4:
            return int(num)
    return token


def _season_retrieve_metadata(
    db: sql.Connection, year_range: YearRange, classification: str | int
) -> dict[str, int | str | date]:
    """Gets metadata for a single season from the database"""
    columns = "min_year, max_year, classification, start_date, stop_date, playlist_spotify_id"
    min_check = "" if year_range[0] else "OR min_year IS NULL"
    max_check = "" if year_range[1] else "OR max_year IS NULL"
    rows = read_rows(
        db.execute(
            f"""
        SELECT {columns} FROM season
        WHERE classification = ? 
            AND (min_year = ? {min_check})
            AND (max_year = ? {max_check})
        """,
            (classification, *year_range),
        ),
        columns,
    )
    try:
        return {k: v for k, v in zip(columns.split(", "), next(rows))}
    except StopIteration:
        raise NoResultsError


def _season_retrieve_min_year(db: sql.Connection) -> int:
    """Finds the earliest release year among ranked tracks"""
    return date.fromisoformat(
        db.execute(
            "SELECT release_day FROM ranking ORDER BY release_day LIMIT 1"
        ).fetchone()[0]
    ).year


def _season_retrieve_tracks(
    db: sql.Connection,
    classification: str | int,
    start_date: date | None,
    stop_date: date | None,
) -> Iterator[str]:
    return flatten(
        cast(list[str], row[0])
        for row in _season_retrieve_rows(
            db,
            "{0}.track_spotify_ids",
            classification,
            start_date,
            stop_date,
            _season_verify_exclusions(classification),
        )
    )


def _season_retrieve_rows(
    db: sql.Connection,
    columns: str,
    classification: str | int,
    start_date: date | None,
    stop_date: date | None,
    exclusion_certifications: Collection[str] = (),
) -> Iterator[tuple]:
    """Gathers projects belonging in a single season in release order

    The `columns` parameter should NOT be constructed from user input,
    as this could open vulnerability to an SQL injection attack. Use {0}
    in place of the table name for automatic substitution (e.g
    "{0}.artist_group, {0}.classification").

    If a date is `None`, the datetime.MAXYEAR and datetime.MINYEAR will
    be used to bound the season.
    """
    if isinstance(classification, int):
        classifications = AUTOSEASON_RANKINGS
        target_table = "ranking"
    else:
        classifications = strray2list(classification)
        if all(c in RANKINGS for c in classifications):
            target_table = "ranking"
        else:
            target_table = "certification"
    start_date, stop_date = start_date or date.min, stop_date or date.max
    cursor = db.execute(
        f"""
        SELECT DISTINCT {columns.replace(';', '').format(target_table)}
        FROM {target_table} LEFT JOIN certification AS exclusion
            ON {target_table}.release_day = exclusion.release_day
                AND {target_table}.artist_names = exclusion.artist_names
                AND {target_table}.name = exclusion.name
                AND exclusion.classification
                    IN {sql_array(exclusion_certifications)}
            LEFT JOIN helper_artist_score
            ON {target_table}.artist_group=helper_artist_score.artist_group
                AND {target_table}.release_day=helper_artist_score.date_from
            LEFT JOIN helper_single
            ON helper_single.single_release_day = {target_table}.release_day
                AND helper_single.artist_names = {target_table}.artist_names
                AND helper_single.single_name = {target_table}.name
                AND helper_single.album_track_names IN (
                    SELECT {target_table}.track_names FROM {target_table}
                    WHERE classification IN {sql_array(classifications)}
                )
        WHERE helper_single.album_track_names IS NULL
            AND {target_table}.classification IN {sql_array(classifications)}
            AND {target_table}.release_day >= ?
            AND {target_table}.release_day < ?
            AND exclusion.classification IS NULL
        ORDER BY {target_table}.release_day ASC, helper_artist_score.score DESC
        """,
        (
            *exclusion_certifications,
            *classifications,
            *classifications,
            start_date,
            stop_date,
        ),
    )
    yield from read_rows(cursor, columns)


def _season_retrieve_year_len(db: sql.Connection, year: int) -> int:
    """Counts the tracks in a year eligible for autoseasons"""
    return len(
        set(
            _season_retrieve_tracks(
                db, 0, beginning_year(year), beginning_year(year + 1)
            )
        )
    )


def _season_store_metadata(
    db: sql.Connection,
    classification: str | int,
    start_date: date | None,
    stop_date: date | None,
    playlist_id: str,
):
    """Saves metadata for a single season in the database"""
    # TODO Duplicate Check
    min_year = start_date.year if start_date else None
    if stop_date is None:
        max_year = None
    elif stop_date.month == 1 and stop_date.day == 1:
        max_year = stop_date.year - 1
    else:
        max_year = stop_date.year
    db.execute(
        "INSERT INTO season VALUES (?, ?, ?, ?, ?, ?)",
        (
            min_year,
            max_year,
            classification,
            start_date,
            stop_date,
            playlist_id,
        ),
    )


def _season_transmit_projects(
    spotify: Spotify, playlist_id: str, season: Iterable[str]
) -> Mob:
    """Uploads a season's tracks to a Spotify playlist"""
    try:
        season = prepend(next(iter(season)), season)
    except StopIteration:
        raise NoResultsError
    mob = cast(Mob, spotify.playlist(playlist_id))
    ss_remove(State(spotify, mob), mob)
    for song_chunk in chunked(season, 100):
        spotify.playlist_add_items(playlist_id, song_chunk)
    return cast(Mob, spotify.playlist(playlist_id))


def _season_verify_exclusions(classification: int | str):
    return EXCLUSION_CERTIFICATIONS - set(strray2list(str(classification)))
