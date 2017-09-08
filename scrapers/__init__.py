
from pymongo import MongoClient

from . import config


def get_db(uri=config.DB):
    return MongoClient(uri).get_default_database()

default_db = get_db()
