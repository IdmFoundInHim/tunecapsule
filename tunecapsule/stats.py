""" Statistical algorithms for TuneCapsule

Copyright (c) 2021 IdmFoundInHim, under MIT License
"""
import sqlite3 as sql
from datetime import date

__all__ = ["cumulative_artist_score", "snapshot_artist_score"]


def cumulative_artist_score(
    db: sql.Connection, spotify_artist_id: str, simulated_date: date | None
) -> float:
    """Represents the volume of quality music for an artist

    The "Street Cred" Score (v1.0)
    -----------------------
    If a project is less than 15 minutes and has fewer than 5 tracks, it is considered a (multi-)single. The ranking will be converted to a score and multiplied by the number of tracks. "A" rankings get 1.8 points/track, "B" rankings get 1.0 pt/trk, and "C" rankings get 0.2 pt/trk.

    All other projects are considered albums and will be scored at the calculated value times the ranking point value (1.8, 1.0, or 0.2). Albums recieve a base value plus up to two length bonuses. The base value is the duration of the album divided by the general average duration of a song, fixed at 3 min 30 sec. All albums below 63 minutes (but at least 15 minutes) recieve a 1 point value bonus, and all albums at least 30 minutes recieve a seperate 1 point value bonus. In other words, albums from 30 minutes to below 63 minutes recieve two bonus value points, while all other albums recieve one bonus value point.
    """
    ...


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
    ...
