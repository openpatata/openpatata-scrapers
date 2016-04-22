
from . import config


def get_database(name=config.DB_NAME):
    from collections import OrderedDict
    from pymongo import MongoClient

    return MongoClient(document_class=OrderedDict)[name]
