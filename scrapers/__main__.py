
"""Cypriot-parliament scraper.

This package collects and stratifies some of what's made available on
the Cypriot parliament's website.  For more information, please see the
README.

Usage: scrapers [-h] <command> [<args> ...]

Commands:
    init            Populate the database
    run             Run a scraping task
    dump            Dump a collection (table) to disk
    clear_cache     Clear the crawler's cache

Options:
    -h --help       Show this screen
"""

import logging
from pathlib import Path

import docopt

from scrapers import config, crawling, db, io
from scrapers.tasks import TASKS

logger = logging.getLogger(__name__)


def _register(fn):
    """Super simple registry and registrar."""
    _register.__dict__[fn.__name__] = fn
    return fn


@_register
def init(args):
    """Usage: scrapers init [-h] [<from_path>]

Populate the database <from_path>, defaulting to './data'.

Options:
    -h --help       Show this screen
"""
    def _init(path):
        db.command('dropDatabase', 1)

        files = map(lambda f: (f, f.parent.stem),
                    Path(path or config.IMPORT_PATH).glob('[!_]*[!_]/*.yaml'))
        for file, collection in files:
            db[collection].insert_one(
                io.YamlManager.load_record(str(file), file.name))

    _init(args['<from_path>'])


@_register
def run(args):
    """Usage: scrapers run [-d|-h] <task>

Run a specified scraper task.

Options:
    -d --debug      Print asyncio debugging messages to stderr
    -h --help       Show this screen
"""
    def _run(task, debug):
        crawling.Crawler(debug=debug)(TASKS[task][0], *TASKS[task][1:])

    _run(args['<task>'], args['--debug'])


@_register
def dump(args):
    """Usage: scrapers dump [-h] <collection> [<at_path>]

Dump a <collection> (table) <at_path>, defaulting to './data-new'.

Options:
    -h --help       Show this screen
"""
    def _dump(collection, path):
        collection = db[collection]
        if not collection.count():
            logger.error("Collection '{}' is empty".format(collection.full_name))
            return

        head = Path(path or config.EXPORT_PATH)/collection.name
        if not head.exists():
            head.mkdir(parents=True)

        for document in collection.find():
            try:
                io.YamlManager.dump_record(document, str(head))
            except io.DumpError as e:
                logger.error(e)

    _dump(args['<collection>'], args['<at_path>'])


@_register
def clear_cache(args):
    """Usage: scrapers clear_cache [-h]

Clear the crawler's cache.

Options:
    -h --help       Show this screen
"""
    def _clear_cache():
        crawling.Crawler.clear_cache()

    _clear_cache()


def main():
    args = docopt.docopt(__doc__, options_first=True)
    try:
        command = _register.__dict__[args['<command>']]
    except KeyError:
        raise docopt.DocoptExit(__doc__)
    else:
        args = docopt.docopt(command.__doc__)
        command(args)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()