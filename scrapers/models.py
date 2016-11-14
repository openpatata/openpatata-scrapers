
"""Models for records used in tasks."""

from pathlib import Path

from . import default_db
from .records import InsertableRecord, SubRecord, RecordRegistry
from .text_utils import date2dato, translit_slugify

registry = RecordRegistry()


class Bill(InsertableRecord):

    collection = default_db.bills
    template = {'_sources': [],
                'actions': [],
                'identifier': None,
                'title': None,
                'titles': []}
    registry = registry
    schema = 'bill'

    def generate__id(self):
        return self.data['identifier']

    def generate_inserts(self, prior_data, merge):
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
                                     '$sort': {'plenary_sitting_id': 1}}},
               '$set': {'title': titles[-1],
                        'titles': titles}}

    class Submission(SubRecord):
        template = {'action': 'submission',
                    'plenary_sitting_id': None,
                    'committees_referred_to': None,
                    'sponsors': None,
                    'title': None}


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
    registry = registry
    schema = 'committee_report'

    def generate__id(self):
        return '_'.join((self.data['date_circulated'] or '_',
                         Path(self.data['url']).stem))

    def generate_inserts(self, prior_data, merge):
        data = yield
        yield {'$set': data}


class ContactDetails(SubRecord):

    template = {'type': None, 'value': None}


class ElectoralDistrict(InsertableRecord):

    collection = default_db.electoral_districts
    template = {'name': {}}
    registry = registry
    schema = 'electoral_district'


class Identifier(SubRecord):

    template = {'identifier': None, 'scheme': None}


class Link(SubRecord):

    template = {'note': {}, 'url': None}


class MP(InsertableRecord):

    collection = default_db.mps
    template = {'_sources': [],
                'birth_date': None,
                'contact_details': [],
                'email': None,
                'gender': None,
                'identifiers': [],
                'image': None,
                'images': [],
                'links': [],
                'name': None,
                'other_names': [],
                'tenures': []}
    registry = registry
    schema = 'mp'

    def generate__id(self):
        return translit_slugify(self.data['name']['el'])

    def generate_inserts(self, prior_data, merge):
        data = yield
        if not merge:
            yield {'$set': data}
            return
        wd = next((i for i in data.get('identifiers', [])
                   if i['scheme'] == 'http://www.wikidata.org/entity/'), None)
        if wd and wd['identifier']:
            yield {'$pull': {'identifiers': {'scheme': 'http://www.wikidata.org/entity/'}}}
            _ = yield
        new_cd = [(i['type'], i['value'], i.get('parliamentary_period_id'))
                  for i in data.get('contact_details', [])]
        cd_to_remove = [i for i in prior_data['contact_details']
                        if (i['type'], i['value'], i.get('parliamentary_period_id')) in new_cd]
        if cd_to_remove:
            yield {'$pullAll': {'contact_details': cd_to_remove}}
            _ = yield
        new_data = {'$addToSet': {k: {'$each': data.pop(k, [])}
                                  for k, v in self.template.items()
                                  if isinstance(v, list)}}
        if next(filter(None, data.values()), None):
            new_data = {**new_data, '$set': data}
        yield new_data
        data = yield
        if sum(1 for i in data['identifiers']
               if i['scheme'] == 'http://www.wikidata.org/entity/') > 1:
            yield {'$pull': {'identifiers': {'identifier': None,
                                             'scheme': 'http://www.wikidata.org/entity/'}}}
        else:
            yield {'$setOnInsert': data}
        data = yield
        yield {'$set': {'contact_details': sorted(data['contact_details'],
                                                  key=lambda i: (i['type'], i['value']))}}

    class Tenure(SubRecord):
        template = {'electoral_district_id': None,
                    'parliamentary_period_id': None,
                    'party_id': None}


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
    registry = registry
    schema = 'parliamentary_period'


class Party(InsertableRecord):

    collection = default_db.parties
    template = {'abbreviation': {}, 'name': {}}
    registry = registry
    schema = 'party'


class PlenarySitting(InsertableRecord):

    collection = default_db.plenary_sittings
    template = {'_sources': [],
                'agenda': {},
                'attendees': [],
                'date': None,
                'links': [],
                'parliamentary_period_id': None,
                'session': None,
                'sitting': None}
    registry = registry
    schema = 'plenary_sitting'

    def generate__id(self):
        date = date2dato(self.data['date']).date().isoformat()
        data = [self.data.get(i) for i in ('parliamentary_period_id', 'session',
                                           'sitting')]
        return '_'.join(map(str, [date] + data))

    def generate_inserts(self, prior_data, merge):
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

    class Link(SubRecord):
        template = {'url': None, 'type': None}

    class PlenaryAgenda(SubRecord):
        template = {'cap1': [], 'cap2': [], 'cap4': []}

    class PlenaryAgendaItem(SubRecord):
        template = {'bill_id': None}


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
    registry = registry
    schema = 'question'

    def generate__id(self):
        return '{}_{}'.format(self.data['identifier'],
                              self.data['_position_on_page'])

    def generate_inserts(self, prior_data, merge):
        data = yield
        yield {'$set': data}
