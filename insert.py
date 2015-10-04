
from collections import OrderedDict
import os

import pymongo
import yaml


def _sort_dicts(val):
    """Recursively transform `dict`s into `OrderedDict`s and sort
    them alphabetically by their key.
    """
    if isinstance(val, dict):
        od = OrderedDict(sorted(val.items()))
        for k in od:
            od[k] = _sort_dicts(od[k])
        return od
    if isinstance(val, list):
        for i in range(len(val)):
            val[i] = _sort_dicts(val[i])
    return val


def main():
    db = pymongo.MongoClient()['openpatata-data']
    db.command('dropDatabase', 1)

    for loc, col in (('./data/bills', 'bills'),
                     ('./data/committee_reports', 'committee_reports'),
                     ('./data/mps', 'mps'),
                     ('./data/plenary_sittings', 'plenary_sittings'),
                     ('./data/questions', 'questions')):
        for filename in os.scandir(loc):
            with open(filename.path) as file:
                data = yaml.load(file.read())
                data['_filename'] = filename.name
                db[col].insert_one(_sort_dicts(data))

if __name__ == '__main__':
    main()
