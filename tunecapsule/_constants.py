"""Package-level Constants for TuneCapsule

Scoring parameters are found in the `stats` module

Copyright (c) 2021 IdmFoundInHim, under MIT License
"""

__all__ = ["DB_DIRECTORY", "DB_LOCATION"]

import os

DB_DIRECTORY = ""
DB_LOCATION = os.path.join(DB_DIRECTORY, "tunecapsule.db")
DB_STRRAY_DELIMITER = "\t"
SPOTIFY_DATE_DELIMITER = "-"
SHA256_ENCODING = "u8"
RANKINGS = ["E", "C", "B", "A"]  # Lowest to Highest
AUTOSEASON_RANKINGS = {"A", "B"}
EXCLUSION_CERTIFICATIONS = {"CHRISTMAS"}
MAX_AUTOSEASON = 366
IDEAL_AUTOSEASON_LENGTH = 80

SEASON_KEYWORDS = {"update"}
