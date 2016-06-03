
"""Models for records used in tasks."""

from pathlib import Path

from .. import default_db
from ..records import InsertableRecord, SubRecord


class Bill(InsertableRecord):

    collection = default_db.bills
    template = {'_sources': [],
                'actions': [],
                'identifier': None,
                'title': None,
                'titles': []}
    required_properties = ('_sources', 'identifier', 'title')

    def generate__id(self):
        return self.data['identifier']

    def generate_inserts(self, merge):
        data = yield
        if not merge:
            yield {'$set': {**data, 'titles': [data['title']]}}
            return
        yield {'$set': data,
               '$addToSet': {'_sources': {'$each': data.pop('_sources')},
                             'actions': {'$each': data.pop('actions', [])},
                             'titles': {'$each': [data.pop('title')]}}}

        data = yield
        titles = data['titles']
        if len(titles) > 1:
            titles = sorted(data['titles'],
                            key=lambda v: v.rpartition(' ')[::-1])
        yield {'$push': {'_sources': {'$each': [], '$sort': 1},
                         'actions': {'$each': [],
                                     '$sort': {'at_plenary_id': 1}}},
               '$set': {'title': titles[-1],
                        'titles': titles}}


class BillActions:

    class Submission(SubRecord):

        template = {'action': 'submission',
                    'at_plenary_id': None,
                    'committees_referred_to': None,
                    'sponsors': None}
        required_properties = ('at_plenary_id', 'committees_referred_to',
                               'sponsors')


class CommitteeReport(InsertableRecord):

    collection = default_db.committee_reports
    template = {'_sources': [],
                'attendees': [],
                'date_circulated': None,
                'date_prepared': None,
                'relates_to': [],
                'text': None,
                'title': None,
                'url': None}
    required_properties = ('_sources', 'title', 'url')

    def generate__id(self):
        return '_'.join((self.data['date_circulated'] or '_',
                         Path(self.data['url']).stem))

    def generate_inserts(self, merge):
        data = yield
        yield {'$set': data}


class ElectoralDistrict(InsertableRecord):

    collection = default_db.electoral_districts
    template = {'name': {}}


class MP(InsertableRecord):

    collection = default_db.mps
    template = {'_sources': [],
                'birth_date': None,
                'email': None,
                'gender': None,
                'image': None,
                'images': [],
                'links': [],
                'name': None,
                'other_names': [],
                'tenures': []}
    required_properties = ('_sources', 'name')

    def generate__id(self):
        from ..text_utils import translit_slugify
        return translit_slugify(self.data['name']['el'])

    def generate_inserts(self, merge):
        data = yield
        if not merge:
            yield {'$set': data}
            return
        yield {'$set': data,
               '$addToSet': {'_sources': {'$each': data.pop('_sources')},
                             **{k: {'$each': data.pop(k, [])}
                                for k in ('images', 'links', 'other_names',
                                          'tenures')}}}

    class Link(SubRecord):
        template = {'note': {}, 'url': None}
        required_properties = ('note', 'url')

    class Tenure(SubRecord):
        template = {'electoral_district_id': None,
                    'parliamentary_period_id': None,
                    'party_id': None}
        required_properties = ('electoral_district_id',
                               'parliamentary_period_id')


class MultilingualField(SubRecord):

    template = {'el': None, 'en': None, 'tr': None}


class OtherName(SubRecord):

    template = {'name': None, 'note': None}


class ParliamentaryPeriod(InsertableRecord):

    collection = default_db.parliamentary_periods
    template = {'_sources': [],
                'number': {},
                'start_date': None,
                'end_date': None}
    required_properties = ('number', 'start_date')


class Party(InsertableRecord):

    collection = default_db.parties
    template = {'abbreviation': {}, 'name': {}}
    required_properties = ('abbreviation', 'name')


class PlenaryAgenda(SubRecord):

    template = {'cap1': [], 'cap2': [], 'cap4': []}


class PlenaryAgendaLink(SubRecord):

    template = {'url': None, 'type': None}
    required_properties = ('url', 'type')


class PlenarySitting(InsertableRecord):

    collection = default_db.plenary_sittings
    template = {'_sources': [],
                'agenda': {},
                'attendees': [],
                'date': None,
                'links': [],
                'parliamentary_period': None,
                'session': None,
                'sitting': None}
    required_properties = ('_sources', 'date', 'parliamentary_period')

    def generate__id(self):
        data = map(self.data.get, ('date', 'parliamentary_period', 'session',
                                   'sitting'))
        return '_'.join(map(str, data))

    def generate_inserts(self, merge):
        data = yield
        if not merge:
            yield {'$set': data}
            return
        yield {'$set': data,
               '$addToSet': {'_sources': {'$each': data.pop('_sources')},
                             'agenda.cap1': {'$each': data.pop('agenda.cap1',
                                                               [])},
                             'agenda.cap4': {'$each': data.pop('agenda.cap4',
                                                               [])},
                             'links': {'$each': data.pop('links', [])}}}

        _ = yield
        yield {'$push': {'_sources': {'$each': [], '$sort': 1},
                         'links': {'$each': [], '$sort': {'type': 1}}}}


class Question(InsertableRecord):

    collection = default_db.questions
    template = {'_position_on_page': None,
                '_sources': [],
                'answers': [],
                'by': [],
                'date': None,
                'heading': None,
                'identifier': None,
                'text': None}
    required_properties = ('_position_on_page', '_sources', 'date', 'heading',
                           'identifier', 'text')

    def generate__id(self):
        return '{}_{}'.format(self.data['identifier'],
                              self.data['_position_on_page'])

    def generate_inserts(self, merge):
        data = yield
        yield {'$set': data}
