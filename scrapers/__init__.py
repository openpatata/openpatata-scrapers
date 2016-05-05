
from . import config


def get_database(uri=config.DB):
    from collections import OrderedDict
    from pymongo import MongoClient

    return MongoClient(uri, document_class=OrderedDict).get_default_database()
