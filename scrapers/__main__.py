
"""Cypriot-parliament scraper.

An XPath and regex soup in a misguided attempt to parse unstructured
structures. This package crawls Parliament's website to collect the
little that is made available and updates existing records on the
openpatata-data repo.

Usage:
  scrapers init
  scrapers run <task> [--debug]
  scrapers dump <collection> <path_on_disk>

Options:
  -h --help  Show this screen.
"""

import asyncio
import logging
import os

from docopt import docopt

from scrapers import db
from scrapers.crawling import Crawler
from scrapers.io import YamlManager
import scrapers.tasks

TASKS = {
    'agendas': (
        scrapers.tasks.process_agenda_index,
        'http://www.parliament.cy/easyconsole.cfm/id/290'),
    'committee_reports': (
        scrapers.tasks.process_committee_report_index,
        'http://www.parliament.cy/easyconsole.cfm/id/220'),
    'committees': (
        scrapers.tasks.process_committee_index,
        'http://www.parliament.cy/easyconsole.cfm/id/183'),
    'mps': (
        scrapers.tasks.process_mp_index,
        'http://www.parliament.cy/easyconsole.cfm/id/186',
        'http://www.parliament.cy/easyconsole.cfm/id/904'),
    'questions': (
        scrapers.tasks.process_question_index,
        'http://www2.parliament.cy/parliamentgr/008_02.htm'),
    'transcript_urls': (
        scrapers.tasks.process_transcript_index,
        'http://www.parliament.cy/easyconsole.cfm/id/159'),
    'transcripts': (
        scrapers.tasks.process_transcripts,
        'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IC.htm',
        'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IDS.htm',
        'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_ID.htm',
        'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IE.htm')}


def dump(collection, path):
    """Dump an entire collection."""
    head = os.path.join(path, collection)
    if not os.path.exists(head):
        os.makedirs(head)
    for doc in db[collection].find(projection={'_id': False}):
        YamlManager.dump(doc, head)


def init():
    """Populate or re-populate a given database from scratch."""
    IMPORTS = [('data/bills',             'bills'),
               ('data/committee_reports', 'committee_reports'),
               ('data/mps',               'mps'),
               ('data/plenary_sittings',  'plenary_sittings'),
               ('data/questions',         'questions')]

    db.command('dropDatabase', 1)
    for path, collection in IMPORTS:
        for filename in os.scandir(path):
            db[collection].insert_one(YamlManager.load(filename.path,
                                                       filename.name))


def run(crawler, debug_flag, task, *task_args):
    """Run a task."""
    loop = asyncio.get_event_loop()
    loop.set_debug(enabled=debug_flag)
    try:
        loop.run_until_complete(task(crawler, *task_args))
    except KeyboardInterrupt:
        pass
    finally:
        crawler.close()


def main():
    """The CLI."""
    args = docopt(__doc__)
    if args['init']:
        init()
    elif args['run']:
        run(Crawler(), args['--debug'],
            TASKS[args['<task>']][0], *TASKS[args['<task>']][1:])
    elif args['dump']:
        dump(args['<collection>'], args['<path_on_disk>'])

logging.basicConfig(level=logging.DEBUG)
main()
