""" Statistical algorithms for TuneCapsule

Copyright (c) 2021 IdmFoundInHim, under MIT License
"""
import sqlite3 as sql
from datetime import date, timedelta
from collections.abc import Iterable

from .utilities import list2strray, read_rows, sql_array

__all__ = ["cumulative_artist_score", "snapshot_artist_score"]


RANK_VALUE = {
    "A": 1.8,
    "B": 1.0,
    "C": 0.2,
}
CERT_VALUE = {
    "💿": lambda tracks: _c_score_standard_certification_value(
        75.0, "A", tracks
    ),
    "🖌": lambda tracks: len(tracks) * 10,
    "🔂": lambda tracks: len(tracks) * 4.2,
}


def cumulative_artist_score(
    db: sql.Connection, spotify_artist_id: str, simulated_date: date | None
) -> float:
    """Represents the volume of quality music for an artist

    The constants not related to classifications have been implemented
    as magic numbers in `_cumulative_artist_score_project_value`.

    The "Street Cred" Score (v1.0)
    -----------------------
    If a project is less than 15 minutes and has fewer than 5 tracks, it is considered a (multi-)single. The ranking will be converted to a score and multiplied by the number of tracks. "A" rankings get 1.8 points/track, "B" rankings get 1.0 pt/trk, and "C" rankings get 0.2 pt/trk.

    All other projects are considered albums and will be scored at the calculated value times the ranking point value (1.8, 1.0, or 0.2). Albums recieve a base value plus up to two length bonuses. The base value is the duration of the album divided by the general average duration of a song, fixed at 3 min 30 sec. All albums below 63 minutes (but at least 15 minutes) recieve a 1 point value bonus, and all albums at least 30 minutes recieve a seperate 1 point value bonus. In other words, albums from 30 minutes to below 63 minutes recieve two bonus value points, while all other albums recieve one bonus value point.
    """
    columns = "classification, track_durations_sec"
    rankings = read_rows(
        db.execute(
            f"""
        SELECT {columns} FROM ranking JOIN helper_artist_group
            ON ranking.artist_group = helper_artist_group.artist_group
                AND helper_artist_group.artist_spotify_id = ?
        WHERE ranking.release_day <= ?
        """,
            (spotify_artist_id, simulated_date),
        ),
        columns,
    )
    ranking_score = sum(
        RANK_VALUE[classification] * _c_score_project_value(track_durations)
        for classification, track_durations in rankings
    )
    certifications = read_rows(
        db.execute(
            f"""
        SELECT {columns} FROM certification JOIN helper_artist_group
            ON certification.artist_group = helper_artist_group.artist_group
            AND helper_artist_group.artist_spotify_id = ?
        WHERE classification IN {sql_array(CERT_VALUE)}
            AND certification.release_day <= ?
        """,
            (spotify_artist_id, *list(CERT_VALUE), simulated_date),
        ),
        columns,
    )
    return sum(
        (
            CERT_VALUE[classification](track_rankings)
            for classification, track_rankings in certifications
        ),
        ranking_score,
    )


def _c_score_project_value(
    track_durations_seconds: Iterable[timedelta],
) -> int | float:
    track_durations = set(track_durations_seconds)
    project_length = len(track_durations)
    project_duration_seconds = sum(track_durations, timedelta(0))
    if project_length < 5 and project_duration_seconds < timedelta(minutes=15):
        return project_length
    else:
        bonus = (
            2
            if timedelta(minutes=15)
            <= project_duration_seconds
            < timedelta(minutes=63)
            else 1
        )
        return (
            project_duration_seconds / timedelta(minutes=3, seconds=30) + bonus
        )


def _c_score_standard_certification_value(
    standard_value: int | float, ranking: str, tracks: Iterable[timedelta]
) -> float:
    return abs(
        standard_value - RANK_VALUE[ranking] * _c_score_project_value(tracks)
    )


def snapshot_artist_score(
    db: sql.Connection, spotify_artist_id: str, simulated_date: date | None
) -> float:
    """Represents the quality of recent music for an artist

    The "Heat Check" Score (v1.0)
    ----------------------
    Each point corresponds to one minute. The score is the amount of
    music released from today (or the simulated date) backwards that
    maintains a high quality level.

    To find the score, sort all known projects of an artist in reverse
    chronological order. Starting with the most recent project, extend
    the range as far back in time such that 1) no C- or E-ranked
    projects are included and 2) at least 70% of the playtime included
    is on an A ranked project. Once the range has been established,
    calculate the total duration of the projects in minutes to find the
    score.
    ----------------------
    """
    columns = "classification, track_durations_sec"
    table = read_rows(
        db.execute(
            f"""
        SELECT {columns} FROM ranking JOIN helper_artist_group
            ON ranking.artist_group = helper_artist_group.artist_group
                AND helper_artist_group.artist_spotify_id = ?
        WHERE ranking.release_day <= ?
        ORDER BY ranking.release_day DESC
        """,
            (spotify_artist_id, simulated_date),
        ),
        columns,
    )
    durations = {"A": timedelta(0), "B": timedelta(0)}
    project_duration_seconds = durations.copy()
    for classification, track_durations_seconds in table:
        if classification not in durations:
            break
        project_duration_seconds[classification] += sum(
            track_durations_seconds, timedelta(0)
        )
        seconds_a = durations["A"] + project_duration_seconds["A"]
        seconds_b = durations["B"] + project_duration_seconds["B"]
        if seconds_a / (seconds_a + seconds_b) > 0.7:
            durations = {"A": seconds_a, "B": seconds_b}
            project_duration_seconds = {"A": timedelta(0), "B": timedelta(0)}
    return (durations["A"] + durations["B"]) / timedelta(minutes=1)


def overall_artist_score(
    db: sql.Connection, spotify_artist_id: str, simulated_date: date | None
) -> float:
    """Combines artist scores into a unified score"""
    return cumulative_artist_score(
        db, spotify_artist_id, simulated_date
    ) + snapshot_artist_score(db, spotify_artist_id, simulated_date)


def store_artist_group_score(
    db: sql.Connection,
    artist_group: Iterable[str],
    simulated_date: date | None,
):
    """Stores score entries for an artist group and constituent artists

    Does not commit changes
    """
    group_score = 0.0
    simulated_date = simulated_date or date.today()
    for artist in artist_group:
        score = overall_artist_score(db, artist, simulated_date)
        db.execute(
            "INSERT INTO helper_artist_score VALUES (?, ?, ?)",
            (artist, simulated_date, score),
        )
        group_score = max(group_score, score)
    db.execute(
        "INSERT INTO helper_artist_score VALUES (?, ?, ?)",
        (list2strray(artist_group), simulated_date, group_score),
    )
