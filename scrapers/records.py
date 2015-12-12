
"""Models for our records."""

from collections import OrderedDict
import itertools
from pathlib import Path
import re

import pymongo

from scrapers import db


class InsertError(Exception):
    """Error raised when a database insert fails."""


class _Record(dict):
    """A base class for our records."""

    def _rekey(self, transform):
        # Recursively apply `transform` to the keys of a _Record, returning
        # a copy of it
        def inner(value, pk=''):
            if isinstance(value, dict):
                for k in value:
                    yield from inner(value[k], transform(pk, k))
            else:
                yield pk, value
        return inner(self)

    def _prepare_insert(self, compact):
        """Subclass to prepare a _Record for `self.insert`."""
        return getattr(self, 'compact' if compact else 'unwrap')().ordered

    @property
    def ordered(self):
        """Traverse the _Record to sort it and all sub-dicts alphabetically."""
        def inner(value):
            if isinstance(value, dict):
                return OrderedDict(itertools.starmap(
                    lambda k, v: (k, inner(v)), sorted(value.items())))
            elif isinstance(value, list):
                return list(map(inner, value))
            return value

        new_type = type(self.__class__.__name__,
                        (self.__class__, OrderedDict), {})
        return new_type(inner(self))

    def unwrap(self):
        r"""Flatten the _Record recursively.

        >>> (_Record({'a': {'b': {'c': [1, 2], 'd': 3}},
        ...           'e': {'f': ''}}).unwrap() ==
        ...  {'a.b.c': [1, 2], 'a.b.d': 3, 'e.f': ''})
        True
        """
        return self.__class__(self._rekey(lambda a, b: (
            '.'.join((a, b)) if a else b)))

    def compact(self):
        r"""Filter out boolean `False` values after flattening.

        This method is useful when updating an existing record, so as to
        not discard nested siblings.

        >>> (_Record({'a': {'b': {'c': 1}}, 'd': {'e': ''}}).compact() ==
        ...  {'a.b.c': 1})
        True
        """
        return self.__class__(filter(lambda i: bool(i[1]),
                                     self.unwrap().items()))

    def insert(self, compact=False, upsert=True):
        """Insert a _Record in the database, returning the resulting
        document or `None`.
        """
        rval = db[self.collection].find_one_and_update(
            filter={'_filename': self['_filename']},
            update=self._prepare_insert(compact), upsert=upsert,
            return_document=pymongo.ReturnDocument.AFTER)
        if not rval:
            raise InsertError('Unable to insert or merge {}'.format(self))
        return rval

    def merge(self):
        """Convenience wrapper around `self.insert` for merging."""
        return self.insert(compact=True, upsert=False)

    @property
    def exists(self):
        return db[self.collection].find_one({'_filename': self['_filename']})

    @classmethod
    def from_template(cls, filename=None, sources=None, update=None):
        """Pre-fill a _Record using its template."""
        value = cls(eval(cls.template))   # Fair bit easier than deep-copying
        value.update({'_filename': filename,
                      '_sources': list(sources) if sources else []})
        if update:
            value.update(update)
        value['_filename'] = value._construct_filename() or value['_filename']
        return value

    def __repr__(self):
        return '<{}: {!r}>'.format(self.__class__.__name__, super().__repr__())


class Bill(_Record):

    collection = 'bills'
    template = """{'identifier': None,
                   'title': None}"""

    def _construct_filename(self):
        return self['_filename'] or self['identifier']

    def _prepare_insert(self, compact):
        value = super()._prepare_insert(compact)
        return {'$set': value}


class CommitteeReport(_Record):

    collection = 'committee_reports'
    template = """{'attendees': [],
                   'date_circulated': None,
                   'date_prepared': None,
                   'relates_to': [],
                   'text': None,
                   'title': None,
                   'url': None}"""

    def _construct_filename(self):
        return '{}_{}'.format(self['date_circulated'] or '_',
                              Path(self['url']).stem)

    def _prepare_insert(self, compact):
        value = super()._prepare_insert(compact)
        return {'$set': value}


class PlenarySitting(_Record):

    collection = 'plenary_sittings'
    template = """{'agenda': {'debate': [],
                              'legislative_work': []},
                   'attendees': [],
                   'date': None,
                   'links': [],
                   'parliamentary_period': None,
                   'session': None,
                   'sitting': None}"""

    def _construct_filename(self):
        filename = self['_filename']
        if not self['date']:
            # See if there was a second plenary on the date, returning its
            # slug
            try:
                date = filename
                date.date and date.slug
            except AttributeError:
                return filename
            else:
                if date.date != date.slug and db[self.collection].find_one(
                        filter={'_filename': date.slug}):
                    return date.slug
                else:
                    return date.date

        # Version same-day sitting filenames from oldest to newest;
        # extraordinary sittings come last. We're doing this bit of filename
        # trickery 'cause:
        # (a) it's probably a good idea if the filenames were to persist; and
        # (b) Parliament similarly version the transcript filenames, meaning
        # that we can avoid downloading and parsing the PDFs (for now, anyway)
        sittings = (
            {(p.get('sitting') or None) for p in
             db[self.collection].find(filter={'date': self['date']})} |
            {self['sitting'] or None})
        sittings = sorted(sittings,
                          key=lambda v: float('inf') if v is None else v)
        sittings = ((('{}_{}'.format(self['date'], index+1)
                      if index > 0 else self['date']),
                     sitting)
                    for index, sitting in enumerate(sittings))
        for filename_, sitting in sittings:
            db[self.collection].find_one_and_update(
                filter={'date': self['date'], 'sitting': sitting},
                update={'$set': {'_filename': filename_}})
            if self['sitting'] == sitting:
                filename = filename_
        return filename

    def _prepare_insert(self, compact):
        value = super()._prepare_insert(compact)
        ins = {'$addToSet': {}, '$set': value}
        source = value.pop('_sources')
        if source:
            ins['$addToSet'].update({'_sources': {'$each': source}})
        if compact:
            debate = value.pop('agenda.debate', None)
            if debate:
                ins['$addToSet'].update({'agenda.debate': {'$each': debate}})

            legislative_work = value.pop('agenda.legislative_work', None)
            if legislative_work:
                ins['$addToSet'].update({'agenda.legislative_work': {
                    '$each': legislative_work}})

            links = value.pop('links', None)
            if links:
                ins['$addToSet'].update({'links': {'$each': links}})
        return ins


class Question(_Record):

    collection = 'questions'
    template = """{'answers': [],
                   'by': [],
                   'date': None,
                   'heading': None,
                   'identifier': None,
                   'text': None}"""

    def _construct_filename(self):
        filename = self['identifier']
        other = db[self.collection].count(
            {'_filename': re.compile(r'{}(_\d+)?'.format(filename))})
        if other:
            return '{}_{}'.format(filename, other+1)
        else:
            return filename

    def _prepare_insert(self, compact):
        value = super()._prepare_insert(compact)
        return {'$set': value}
