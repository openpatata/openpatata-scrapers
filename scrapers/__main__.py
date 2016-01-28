
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

import itertools
import logging
from pathlib import Path
from textwrap import dedent

import docopt

from . import config, db, io

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
    """Usage: scrapers init [-h] [<from_path> [<import> ...]]

    Populate the database <from_path>, defaulting to './data', and
    enclosing <import> directories.

    Options:
        -h --help       Show this screen
    """
    def _init(import_path, dirs):
        db.command('dropDatabase', 1)

        files = itertools.chain.from_iterable(map(
            lambda dir_: zip(Path(import_path or
                                  config.IMPORT_PATH, dir_).iterdir(),
                             itertools.repeat(dir_)),
            dirs or config.IMPORT_DIRS))
        for path, collection in files:
            db[collection].insert_one(io.YamlManager.load_record(str(path),
                                                                 path.stem))

    _init(args['<from_path>'], args['<import>'])


@_register
def run(args):
    """Usage: scrapers run [-d|-h] <task>

    Run a specified scraper task.

    Options:
        -d --debug      Print asyncio debugging messages to stderr
        -h --help       Show this screen
    """
    from scrapers import crawling
    from scrapers.tasks import TASKS

    def _run(task, debug):
        if task not in TASKS:
            raise docopt.DocoptExit('Task must to be one of: {}'.format(
                '; '.join(sorted(TASKS))))
        crawling.Crawler(debug=debug)(TASKS[task])

    _run(args['<task>'], args['--debug'])


@_register
def dump(args):
    """Usage: scrapers dump [-h] [--location=<location>] [<collections> ...]

    Dump a <collection> (table) at <location>, defaulting to './data-new'.

    Options:
        --location=<location>   Path on disk where to gently deposit the data
        -h --help               Show this screen
    """
    def _dump(collection, location):
        collection = db[collection]
        if collection.count() == 0:
            raise docopt.DocoptExit(
                'Collection {!r} is empty'.format(collection.full_name))

        head = Path(location or config.EXPORT_PATH)/collection.name
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
def clear_cache(_):
    """Usage: scrapers clear_cache [-h]

    Clear the crawler's cache.

    Options:
        -h --help       Show this screen
    """
    from scrapers import crawling
    crawling.Crawler.clear_cache()


def main():
    args = docopt.docopt(__doc__, options_first=True)
    try:
        command = _register.__dict__[args['<command>']]
    except KeyError:
        raise docopt.DocoptExit('Command must to be one of: {}'.format(
            '; '.join(sorted(_register.__dict__))))
    else:
        args = docopt.docopt(command.__doc__)
        command(args)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
