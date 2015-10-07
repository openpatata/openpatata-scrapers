
from collections import OrderedDict
import os

import yaml

# Use block style w/ multi-line strings
yaml.CDumper.add_representer(str, lambda dumper, value: (
    dumper.represent_scalar('tag:yaml.org,2002:str', value,
                            style='|' if '\n' in value else None)))


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


def _yaml_dump(data, path):
    """Save a document to disk as YAML."""
    head = os.path.dirname(path)
    if not os.path.exists(head):
        os.makedirs(head)
    with open(path, 'w') as file:
        yaml.dump(data, file, Dumper=yaml.CDumper,
                  allow_unicode=True, default_flow_style=False)


def dump_collection(db, collection, path):
    """Save an entire collection."""
    for doc in db[collection].find(projection={'_id': False}):
        filename = doc.pop('_filename')
        _yaml_dump(doc, os.path.join(path, collection, filename))


def populate_db(db):
    db.command('dropDatabase', 1)

    for loc, col in (('./data/bills', 'bills'),
                     ('./data/committee_reports', 'committee_reports'),
                     ('./data/mps', 'mps'),
                     ('./data/plenary_sittings', 'plenary_sittings'),
                     ('./data/questions', 'questions')):
        for filename in os.scandir(loc):
            with open(filename.path) as file:
                data = yaml.load(file.read(), Loader=yaml.CLoader)
                data['_filename'] = filename.name
                db[col].insert_one(_sort_dicts(data))
