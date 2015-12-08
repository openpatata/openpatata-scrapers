
"""Models for our records."""

from collections import OrderedDict
import itertools
import re

import pymongo

from scrapers import db
from scrapers.text_utils import Translit, truncate_slug


class _Unique(type):

    def __new__(cls, *args, **kwargs):
        cls = super().__new__(cls, *args, **kwargs)
        cls.seen = set()
        return cls


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

    def _prepare(self, compact):
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

        new_type = type(type(self).__name__,
                        (type(self), OrderedDict), {})
        return new_type(inner(self))

    def unwrap(self):
        r"""Flatten the _Record recursively.

        >>> (_Record({'a': {'b': {'c': [1, 2], 'd': 3}},
        ...           'e': {'f': ''}}).unwrap() ==
        ...  {'a.b.c': [1, 2], 'a.b.d': 3, 'e.f': ''})
        True
        """
        return type(self)(self._rekey(lambda a, b: ('.'.join((a, b))
                                                    if a else b)))

    def compact(self):
        r"""Filter out booleans `False` values after flattening.

        This method is useful when updating an existing record, so as to
        not discard nested siblings.

        >>> (_Record({'a': {'b': {'c': 1}}, 'd': {'e': ''}}).compact() ==
        ...  {'a.b.c': 1})
        True
        """
        return type(self)(filter(lambda i: bool(i[1]), self.unwrap().items()))

    def insert(self,
               filter_=None, compact=False, upsert=True):
        """Insert a _Record in the database, returning the resulting
        document or `None`.
        """
        return db[self.collection].find_one_and_update(
            filter_ or {'_filename': self['_filename']},
            update=self._prepare(compact), upsert=upsert,
            return_document=pymongo.ReturnDocument.AFTER)

    def merge(self, filter_=None):
        """Convenience wrapper around `self.insert` for merging."""
        return self.insert(filter_, compact=True, upsert=False)

    @classmethod
    def from_template(cls, _filename=None, update=None):
        """Pre-fill a _Record using its template."""
        value = cls(eval(cls.template))   # Fair bit easier than deep-copying
        if _filename:
            value.update({'_filename': _filename})
        if update:
            value.update(update)
        return value


class Bill(_Record):

    collection = 'bills'
    template = """{'_filename': None,
                   'identifier': None,
                   'title': None}"""

    def _prepare(self, compact):
        value = super()._prepare(compact)
        value['_filename'] = value['identifier']
        return {'$set': value}


class CommitteeReport(_Record):

    collection = 'committee_reports'
    template = """{'_filename': None,
                   'attendees': [],
                   'date_circulated': None,
                   'date_prepared': None,
                   'relates_to': [],
                   'text': None,
                   'title': None,
                   'url': None}"""

    def _construct_filename(self, value):
        slug = truncate_slug(Translit.slugify(value['title']))

        other = db.committee_reports.find_one({'url': value['url']})
        if other:
            return other['_filename']
        other = db.committee_reports.count(
            {'_filename': re.compile(r'{}(_\d+)?'.format(slug))})
        if other:
            return '{}_{}'.format(slug, other+1)
        else:
            return slug

    def _prepare(self, compact):
        value = super()._prepare(compact)
        value['_filename'] = self._construct_filename(value)
        return {'$set': value}


class PlenarySitting(_Record):

    collection = 'plenary_sittings'
    template = """{'_filename': None,
                   'agenda': {'debate': [],
                              'legislative_work': []},
                   'attendees': [],
                   'date': None,
                   'links': [],
                   'parliamentary_period': None,
                   'session': None,
                   'sitting': None}"""

    def _version_filename(self, value):
        # Version same-day sitting filenames from oldest to newest;
        # extraordinary sittings come last. We're doing this bit of filename
        # trickery 'cause:
        # (a) it's probably a good idea if the filenames were to persist; and
        # (b) Parliament similarly version the transcript filenames, meaning
        # that we can avoid downloading and parsing the PDFs (for now, anyway)
        if 'date' not in value:
            return

        sittings = (
            {(self.from_template(None, p)['sitting'] or None) for p in
             db[self.collection].find(filter={'date': value['date']})} |
            {value['sitting'] or None})
        sittings = sorted(sittings,
                          key=lambda v: float('inf') if v is None else v)
        for c, sitting in enumerate(sittings):
            if c > 0:
                _filename = '{}_{}'.format(value['date'], c+1)
            else:
                _filename = value['date']
            db[self.collection].find_one_and_update(
                filter={'date': value['date'], 'sitting': sitting},
                update={'$set': {'_filename': _filename}})

            if value['sitting'] == sitting:
                value['_filename'] = _filename

    def _prepare(self, compact):
        value = super()._prepare(compact)
        self._version_filename(value)

        ins = {'$set': value}
        if compact:
            links = value.pop('links', None)
            if links:
                ins.update({'$addToSet': {'links': {'$each': links}}})

        return ins

    @classmethod
    def select_date(cls, date):
        """See if there was a second plenary on the `date`, returning its
        slug.
        """
        if date.date != date.slug and \
                db[cls.collection].find_one(filter={'_filename': date.slug}):
            return date.slug
        else:
            return date.date


class Question(_Record,
               metaclass=_Unique):

    collection = 'questions'
    template = """{'_filename': None,
                   'answers': [],
                   'by': [],
                   'date': None,
                   'heading': None,
                   'identifier': None,
                   'text': None}"""

    def _prepare(self, compact):
        value = super()._prepare(compact)
        return {'$set': value}
