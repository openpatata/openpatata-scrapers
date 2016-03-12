
"""Models for records used in tasks."""

from collections import OrderedDict
import itertools as it
import json
from pathlib import Path

from ..records import InsertError, InsertableRecord, SubRecord


class Bill(InsertableRecord):

    collection = 'bills'
    template = {'_sources': [],
                'actions': [],
                'identifier': None,
                'title': None,
                'other_titles': []}
    required_properties = ('_sources', 'identifier', 'title', 'other_titles')

    def generate__id(self):
        return self.data['identifier']

    def generate_inserts(self, merge):
        data = yield
        if not merge:
            yield {'$set': data}
            return
        yield {'$set': data,
               '$addToSet': {'_sources': {'$each': data.pop('_sources')},
                             'actions': {'$each': data.pop('actions', [])},
                             'other_titles': {'$each': data.pop('other_titles')
                                              }}}

        data = yield
        titles = sorted([data['title']] + data['other_titles'],
                        key=lambda v: tuple(reversed(v.rpartition(' '))))
        yield {'$push': {'_sources': {'$each': [], '$sort': 1},
                         'actions': {'$each': [],
                                     '$sort': {'at_plenary_id': 1}}},
               '$set': {'title': titles[-1],
                        'other_titles': titles[:-1]}}


class BillActions:

    class Submission(SubRecord):

        template = {'action': 'submission',
                    'at_plenary_id': None,
                    'committees_referred_to': None,
                    'sponsors': None}
        required_properties = ('at_plenary_id', 'committees_referred_to',
                               'sponsors')


class CommitteeReport(InsertableRecord):

    collection = 'committee_reports'
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


class MP(InsertableRecord):

    collection = 'mps'

    @classmethod
    def export_all_to_popolo(cls, locale):
        areas = cls.collection.aggregate([
            {'$unwind': '$tenures'},
            {'$group': {'_id': 0,
                        'areas': {'$addToSet': '$tenures.electoral_district'}}},
            {'$project':
                {'_id': 0,
                 'areas': {'$map': {'input': '$areas', 'as': 'area',
                                    'in': {'id': {'$concat': ['electoral_district/',
                                                              {'$toLower': '$$area.en'}]},
                                           'name': '$$area.'+locale}}}}}])
        memberships = cls.collection.aggregate([
            {'$unwind': '$tenures'},
            {'$project':
                {'_id': 0,
                 'area_id': {'$concat': ['electoral_district/',
                                         {'$toLower': '$tenures.electoral_district.en'}]},
                 'end_date': '$tenures.end_date',
                 'on_behalf_of_id': {'$concat': ['parliamentary_group/',
                                                 '$tenures.parliamentary_group_id']},
                 'organization': {'$const': {'name': 'Βουλή των Αντιπροσώπων'}},
                 'person_id': {'$concat': ['politician/', '$_id']},
                 'start_date': '$tenures.start_date'}
             },
            {'$group': {'_id': 0, 'memberships': {'$push': '$$ROOT'}}},
            {'$project': {'_id': 0, 'memberships': '$memberships'}}])
        organisations = \
            cls.collection.database.parliamentary_groups.aggregate([
                {'$project': {'_id': 0,
                              'abbreviation': '$abbreviation.'+locale,
                              'id': {'$concat': ['parliamentary_group/',
                                                 '$_id']},
                              'name': '$name.'+locale}},
                {'$group': {'_id': 0, 'organizations': {'$push': '$$ROOT'}}},
                {'$project': {'_id': 0, 'organizations': '$organizations'}}])
        persons = cls.collection.aggregate([
            {'$project':
                {'_id': 0,
                 'id': {'$concat': ['politician/', '$_id']},
                 'birth_date': {'$ifNull': ['$birth_date', '']},
                 'email': {'$ifNull': ['$email', '']},
                 'gender': {'$ifNull': ['$gender', '']},
                 'image': {'$ifNull': ['$mugshot', '']},
                 'links': {'$ifNull': [{'$map': {'input': '$links', 'as': 'link',
                                                 'in': {'note': '$$link.note.'+locale,
                                                        'url': '$$link.url'}}}, []]},
                 'name': '$name.'+locale}
             },
            {'$group': {'_id': 0, 'persons': {'$push': '$$ROOT'}}},
            {'$project': {'_id': 0, 'persons': '$persons'}}])

        doc = OrderedDict()
        for i in it.chain(areas, memberships, organisations, persons):
            doc.update(i)
        return json.dumps(doc, ensure_ascii=False, indent=2)


class PlenaryAgenda(SubRecord):

    template = {'cap1': [], 'cap2': [], 'cap4': []}


class PlenaryAgendaLink(SubRecord):

    template = {'url': None, 'type': None}
    required_properties = ('url', 'type')


class PlenarySitting(InsertableRecord):

    collection = 'plenary_sittings'
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

    collection = 'questions'
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
