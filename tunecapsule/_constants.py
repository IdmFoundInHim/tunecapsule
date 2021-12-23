""" Package-level Constants for TuneCapsule

Copyright (c) 2021 IdmFoundInHim, under MIT License
"""
__all__ = ["DB_DIRECTORY", "DB_LOCATION"]

import os

DB_DIRECTORY = os.path.join("env", "db")
DB_LOCATION = os.path.join(DB_DIRECTORY, "tunecapsule.db")
DB_STRRAY_DELIMITER = "\t"
SPOTIFY_DATE_DELIMITER = "-"
SHA256_ENCODING = "u8"
RANKINGS = set("ABCE")
