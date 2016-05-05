
"""Cypriot-parliament scraper.

This package collects and stratifies some of what's made available on
the Cypriot parliament's website.  For more information, please see the
README.

Usage: scrapers [-v] <command> [<args> ...]

Commands:
    init            Populate the database
    run             Run a scraping task
    dump            Dump a collection (table) to disk
    clear_cache     Clear the crawler's cache

Options:
    -h --help       Show this screen
    -v --verbose    Print error messages of all levels
"""

import itertools as it
import logging
from pathlib import Path
from textwrap import dedent

from docopt import docopt, DocoptExit

from . import get_database, io

logger = logging.getLogger(__name__)


def _register(fn):
    """Super simple registry and registrar."""
    fn.__doc__ = fn.__doc__.partition('\n')
    fn.__doc__ = fn.__doc__[:-1] + (dedent(fn.__doc__[-1]),)
    fn.__doc__ = ''.join(fn.__doc__)
    _register.__dict__[fn.__name__] = fn
    return fn


@_register
def init(args):
    """Usage: scrapers init [<from_path> [<import> ...]]

    Populate the database <from_path>, defaulting to './data', and
    enclosing <import> directories.

    Options:
        -h --help       Show this screen
    """
    def _init(import_path, dirs):
        db = get_database()
        db.command('dropDatabase')

        files = it.chain.from_iterable(map(
            lambda dir_: zip(Path(import_path, dir_).iterdir(),
                             it.repeat(dir_)),
            dirs))
        for path, collection in files:
            db[collection].insert_one(io.YamlManager.load_record(str(path),
                                                                 path.stem))

    _init(args['<from_path>'] or './data',
          args['<import>'] or ('bills', 'mps', 'plenary_sittings', 'questions'))


@_register
def run(args):
    """Usage: scrapers run [-d] <task>

    Run a specified scraper task.

    Options:
        -d --debug      Print asyncio debugging messages to `stderr`
        -h --help       Show this screen
    """
    from scrapers import crawling
    from scrapers.tasks import TASKS

    def _run(task, debug):
        if task not in TASKS:
            raise DocoptExit('Available tasks are: ' +
                             '; '.join(sorted(TASKS)))
        crawling.Crawler(debug=debug)(TASKS[task])

    _run(args['<task>'], args['--debug'])


@_register
def dump(args):
    """Usage: scrapers dump [--location=<location>] [<collections> ...]

    Dump a <collection> (table) at <location>, defaulting to './data-new'.

    Options:
        --location=<location>   Path on disk where to gently deposit the data
                                    [default: ./data-new]
        -h --help               Show this screen
    """
    def _dump(collection, location):
        collection = get_database()[collection]
        if collection.count() == 0:
            raise DocoptExit('Collection {!r} is empty'
                             .format(collection.full_name))

        head = Path(location)/collection.name
        if not head.exists():
            head.mkdir(parents=True)
        for document in collection.find():
            try:
                io.YamlManager.dump_record(document, str(head))
            except io.DumpError as e:
                logger.error(e)

    for collection in args['<collections>']:
        _dump(collection, args['--location'])


@_register
def export(args):
    """Usage: scrapers export [--locale=<locale>]

    Export the MP (person) collection to Popolo JSON.  This follows a similar
    format to everypolitician's (<https://github.com/everypolitician>).

    Options:
        --locale=<locale>   Localise in <locale> [default: el]
        -h --help           Show this screen
    """
    from .tasks._models import export_all_to_popolo
    print(export_all_to_popolo(args['--locale']))


@_register
def clear_cache(_):
    """Usage: scrapers clear_cache

    Clear the crawler's cache.

    Options:
        -h --help       Show this screen
    """
    from scrapers import crawling
    crawling.Crawler.clear_cache()


def main():
    args = docopt(__doc__, options_first=True)
    if args['--verbose']:
        logging.basicConfig(level=logging.DEBUG)
    try:
        command = _register.__dict__[args['<command>']]
    except KeyError:
        raise DocoptExit('Available commands are: ' +
                         '; '.join(sorted(_register.__dict__)))
    else:
        args = docopt(command.__doc__,
                      argv=[args['<command>']] + args['<args>'])
        command(args)

if __name__ == '__main__':
    main()
