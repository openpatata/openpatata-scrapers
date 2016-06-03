
"""\
Cypriot-parliament scraper.

This package collects and stratifies some of what's made available on
the Cypriot parliament's website.  For more information, please see the
README.

Usage: scrapers [-v] <command> [<args> ...]

Commands:
    init            Populate the database
    run             Run a scraping task
    dump            Dump a collection (table) on disk
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

from . import default_db, io

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
    """Usage: scrapers init [--keep-db] [<from-folders> ...]

    Populate the database <from-folders>, defaulting to just './data/mps'.

    Options:
        --keep-db       Don't drop the database before importing
        -h --help       Show this screen
    """
    def _init(folders, keep_db):
        if not keep_db:
            default_db.command('dropDatabase')

        files = it.chain.from_iterable(map(
            lambda folder: (
                lambda folder: zip(folder.glob('*.yaml'),
                                   it.repeat(folder.stem)))(Path(folder)),
            folders))
        for path, collection in files:
            default_db[collection].insert_one(
                io.YamlManager.load_record(str(path), path.stem))

    _init(args['<from-folders>'] or ('./data/mps',), args['--keep-db'])


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
    """Usage: scrapers dump [--location=<location>] (<collections> ...)

    Dump a <collection> (table) at <location>, defaulting to './data-new'.

    Options:
        --location=<location>   Path on disk where to gently deposit the data
                                    [default: ./data-new]
        -h --help               Show this screen
    """
    def _dump(collection, location):
        collection = default_db[collection]
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
    """Usage: scrapers export [--format=<format>] <collection>

    Options:
        --format=<format>   Export format [default: csv]
        -h --help           Show this screen
    """
    def _export(collection, format_):
        from .tasks import _is_subclass, _models
        export_fns = {m.collection.name: m.export
                      for m in _models.__dict__.values()
                      if _is_subclass(m, _models.InsertableRecord)}
        try:
            print(export_fns[collection](format_))
        except KeyError:
            raise DocoptExit('No collection {!r}'.format(collection))

    _export(args['<collection>'], args['--format'])


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
