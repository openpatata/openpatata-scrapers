
"""Models for records used in tasks."""

from pathlib import Path
import re

from ..records import BaseRecord, InsertError


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
                             'actions': {'$each': data.pop('actions', [])},
                             'other_titles': {'$each': data.pop('other_titles')
                                              }}}
        other_titles = sorted(self._value_in_db['other_titles'],
                              key=lambda v: tuple(reversed(v.rpartition(' '))))
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
        other = self.collection\
            .count({'_filename': re.compile(r'{}(_\d+)?$'.format(filename))})
        if other:
            filename = '_'.join((filename, str(other + 1)))
        return {'_filename': filename},

    def _prepare_inserts(self, data, merge):
        return {'$set': data},
