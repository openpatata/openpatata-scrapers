
from collections import OrderedDict

from pymongo import MongoClient

from scrapers import config

db = MongoClient(document_class=OrderedDict)[config.DB_NAME]
