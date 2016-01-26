
"""Models for our records."""

from collections import OrderedDict
from copy import deepcopy
import itertools as it
from pathlib import Path
import re

import pymongo

from . import db
from .misc_utils import starfilter


class InsertError(Exception):
    """Error raised when a database insert fails."""


class BaseRecord:
    """A base class for our records."""

    def __init__(self, data, is_raw=False):
        self._value_in_db = None

        if is_raw is True:
            self.data = deepcopy(data)
            return
        self.data = deepcopy(self.template)
        self.data.update(data)
        for update in self._on_init_transforms():
            self.data.update(update)
        if not all(map(self.data.get, it.chain(self.required_properties,
                                               ('_filename', '_sources')
                                               ))):
            raise ValueError(', '.join(map(repr, self.required_properties)) +
                             ", '_filename' and '_sources' are required in " +
                             repr(self))

    def __repr__(self):
        return '<{}: {!r}>'.format(self.__class__.__name__, self.data)

    def _compact(self):
        """Filter out boolean `False` values, returning a copy of the `BaseRecord`.

        >>> (BaseRecord({'a': {'b': {'c': 1}}, 'd': ''}, is_raw=True)
        ...  ._compact().data == {'a': {'b': {'c': 1}}})
        True
        >>> (BaseRecord({'a': {'b': {'c': 1}}, 'd': {'e': ''}}, is_raw=True)
        ...  ._unwrap()._compact().data == {'a.b.c': 1})
        True
        """
        return self.__class__(
            self.data.__class__(starfilter(lambda _, v: v, self.data.items())),
            is_raw=True)

    def _on_init_transforms(self):
        raise NotImplementedError

    def _prepare_inserts(self):
        raise NotImplementedError

    def _sort(self):
        """Traverse `self.data` to sort it and all sub-dicts alphabetically."""
        def sort(value):
            if isinstance(value, dict):
                return OrderedDict(it.starmap(lambda k, v: (k, sort(v)),
                                              sorted(value.items())))
            elif isinstance(value, list):
                return list(map(sort, value))
            return value

        return self.__class__(OrderedDict(sort(self.data)), is_raw=True)

    def _unwrap(self):
        """Flatten the `BaseRecord` recursively, returning a copy of it.

        >>> (BaseRecord({'a': {'b': {'c': [1, 2], 'd': 3}}, 'e': {'f': ''}},
        ...          is_raw=True)
        ...  ._unwrap().data == {'a.b.c': [1, 2], 'a.b.d': 3, 'e.f': ''})
        True
        """
        def rekey(value, pk=''):
            if isinstance(value, dict):
                for k in value:
                    yield from rekey(value[k], '.'.join((pk, k)) if pk else k)
            else:
                yield pk, value

        return self.__class__(self.data.__class__(rekey(self.data)),
                              is_raw=True)

    @property
    def exists(self):
        """See whether a `BaseRecord` with the same `_filename` already exists."""
        filter_ = {'_filename': self.data['_filename']}
        return bool(db[self.collection].find_one(filter=filter_))

    def insert(self, merge=False):
        """Insert a `BaseRecord` in the database.

        `insert` returns the resulting document on success and
        raises `InsertError` on failure.
        """
        data = self._unwrap()._sort().data
        if merge is True:
            data = self.__class__(data, is_raw=True)._compact().data

        filter_ = {'_filename': self.data['_filename']}
        if merge is not True:
             db[self.collection].find_one_and_delete(filter=filter_)

        for insert in self._prepare_inserts(data, merge):
            return_value = self._value_in_db = \
                db[self.collection].find_one_and_update(
                    filter=filter_,
                    update=insert,
                    upsert=not merge,
                    return_document=pymongo.ReturnDocument.AFTER)
            if not return_value:
                raise InsertError('Unable to insert or merge ' + repr(self))
        return return_value


class Bill(BaseRecord):

    collection = 'bills'
    template = {'actions': [],
                'identifier': None,
                'title': None,
                'other_titles': []}
    required_properties = ('identifier', 'title', 'other_titles')

    def _on_init_transforms(self):
        return {'_filename': self.data['identifier']},

    def _prepare_inserts(self, data, merge):
        yield {'$set': data,
               '$addToSet': {'_sources': {'$each': data.pop('_sources')},
                             'actions': {'$each': data.pop('actions', [])}
                             'other_titles': {'$each': data.pop('other_titles')
                                              }}}
        other_titles = sorted(self._value_in_db['other_titles'],
                              key=lambda v: tuple(reversed(v.rpartition(' ')))
                              )
        yield {'$push': {'_sources': {'$each': [], '$sort': 1},
                         'actions': {'$each': [], '$sort': {'at_plenary': 1}}},
               '$set': {'other_titles': other_titles}}
        # <Pause to allow for the value of `self._value_in_db` to be updated>
        yield {'$set': {'title': self._value_in_db['other_titles'][-1]}}


class CommitteeReport(BaseRecord):

    collection = 'committee_reports'
    template = {'attendees': [],
                'date_circulated': None,
                'date_prepared': None,
                'relates_to': [],
                'text': None,
                'title': None,
                'url': None}
    required_properties = ('title', 'url')

    def _on_init_transforms(self):
        return {'_filename': '_'.join((self.data['date_circulated'] or '_',
                                       Path(self.data['url']).stem))},

    def _prepare_inserts(self, data, merge):
        return {'$set': data},


class PlenarySitting(BaseRecord):

    collection = 'plenary_sittings'
    template = {'agenda': {'cap1': [], 'cap2': [], 'cap4': []},
                'attendees': [],
                'date': None,
                'links': [],
                'parliamentary_period': None,
                'session': None,
                'sitting': None}
    required_properties = ('date', 'parliamentary_period')

    def _on_init_transforms(self):
        data = self.data
        return {'_filename': '_'.join(map(str, (data['date'],
                                                data['parliamentary_period'],
                                                data['session'],
                                                data['sitting'])))},

    def _prepare_inserts(self, data, merge):
        ins = {'$set': data,
               '$addToSet': {'_sources': {'$each': data.pop('_sources')}}}
        if merge:
            cap1 = data.pop('agenda.cap1', None)
            if cap1:
                ins['$addToSet'].update({'agenda.cap1': {'$each': cap1}})
            cap4 = data.pop('agenda.cap4', None)
            if cap4:
                ins['$addToSet'].update({'agenda.cap4': {'$each': cap4}})
            links = data.pop('links', None)
            if links:
                ins['$addToSet'].update({'links': {'$each': links}})
        return ins, {'$push': {'_sources': {'$each': [], '$sort': 1},
                               'links': {'$each': [], '$sort': {'type': 1}}}}


class Question(BaseRecord):

    collection = 'questions'
    template = {'answers': [],
                'by': [],
                'date': None,
                'heading': None,
                'identifier': None,
                'text': None}
    required_properties = ('date', 'heading', 'identifier', 'text')

    def _on_init_transforms(self):
        filename = self.data['identifier']
        other = db[self.collection]\
            .count({'_filename': re.compile(r'{}(_\d+)?$'.format(filename))})
        if other:
            filename = '_'.join((filename, str(other + 1)))
        return {'_filename': filename},

    def _prepare_inserts(self, data, merge):
        return {'$set': data},
