""" Database Initializer for TuneCapsule

Copyright (c) 2021 IdmFoundInHim, under MIT License
"""
import os
import sqlite3
import sys

from ._constants import DB_LOCATION
from ._dbinit import initialize_database

print("To use tunecapsule, run 'python -m streamsort tunecapsule'")
print("Initializing Database...")
if os.path.exists(DB_LOCATION) and "reset" == sys.argv[1]:
    os.remove(DB_LOCATION)
try:
    initialize_database()
    print("Success!")
except sqlite3.OperationalError:
    print("Use 'python -m tunecapsule reset' to wipe the database")
