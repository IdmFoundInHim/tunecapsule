""" Database Initializer for TuneCapsule

Copyright (c) 2021 IdmFoundInHim, under MIT License
"""
import os

from ._constants import DB_LOCATION
from ._dbinit import initialize_database

if os.path.exists(DB_LOCATION):
    os.remove(DB_LOCATION)
initialize_database()
