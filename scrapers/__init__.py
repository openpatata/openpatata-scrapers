
from . import config


def get_database(name):
    from collections import OrderedDict
    from pymongo import MongoClient

    return MongoClient(document_class=OrderedDict)[name]

db = get_database(config.DB_NAME)
