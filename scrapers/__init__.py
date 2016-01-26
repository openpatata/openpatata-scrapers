
from collections import OrderedDict

from pymongo import MongoClient

from . import config

db = MongoClient(document_class=OrderedDict)[config.DB_NAME]
