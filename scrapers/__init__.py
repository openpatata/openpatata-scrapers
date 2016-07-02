
import builtins
import logging

builtins.logger = logging.getLogger(__name__)


class _cached_db:

    def __init__(self, fn):
        self.fn_name = fn.__name__

    def __get__(self, _, owner):
        db_instance = owner.get()
        setattr(owner, self.fn_name, db_instance)
        return db_instance


class Db:

    def get(uri=None):
        from collections import OrderedDict
        from pymongo import MongoClient

        if not uri:
            from . import config
            uri = config.DB
        return MongoClient(uri, document_class=OrderedDict)\
            .get_default_database()

    @_cached_db
    def default_db(): pass

default_db = Db.default_db
