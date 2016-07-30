
"""\
Cypriot-parliament scraper.

This package collects and stratifies some of what's made available on
the Cypriot parliament's website.  For more information, please see the
README.

Usage: scrapers [-v] <command> [<args> ...]

Commands:
    cache--clear   Clear the crawler's cache
    dump           Dump documents in a collection as YAML
    cache--dump    Dump the crawler's cache on disk
    export         Export a collection to CSV or JSON
    init           Populate the database
    run            Run a scraping task

Options:
    -h --help       Show this screen
    -v --verbose    Print error messages of all levels
"""

import logging
from pathlib import Path
import textwrap

from docopt import docopt, DocoptExit
import pygit2

from . import crawling, default_db, io, models, tasks


def _register(fn, name=None):
    """Super simple registry and registrar."""
    if isinstance(fn, str):
        return lambda v: _register(v, name=fn)
    fn.__doc__ = fn.__doc__.partition('\n')
    fn.__doc__ = fn.__doc__[:-1] + (textwrap.dedent(fn.__doc__[-1]),)
    fn.__doc__ = ''.join(fn.__doc__)
    _register.__dict__[name or fn.__name__] = fn
    return fn


@_register
def init(args):
    """Usage: scrapers init [--keep-db] [<from-folders> ...]

    Populate the database <from-folders>, defaulting to just './data/mps'.

    Options:
        --keep-db       Don't drop the database before importing
        -h --help       Show this screen
    """
    if not args['--keep-db']:
        default_db.command('dropDatabase')

    files = ((f, d.stem)
             for d in map(Path, args['<from-folders>'] or ('./data/mps',))
             for f in d.glob('*.yaml'))
    for path, collection in files:
        default_db[collection].insert_one(io.YamlManager.load_record(str(path),
                                                                     path.stem))


@_register
def run(args):
    """Usage: scrapers run [-d] <task>

    Run a specified scraper task.

    Options:
        -d --debug      Print asyncio debugging messages to `stderr`
        -h --help       Show this screen
    """
    if args['<task>'] not in tasks.TASKS:
        raise DocoptExit('\n'.join(['Available tasks are: '] +
                                   ['\t' + i
                                    for i in textwrap
                                     .wrap('; '.join(sorted(tasks.TASKS)))] +
                                   ['']))
    crawling.Crawler(debug=args['--debug'])(tasks.TASKS[args['<task>']])


@_register
def dump(args):
    """Usage: scrapers dump [--location=<location>] (<collections> ...)

    Dump a <collection> (table) at <location>, defaulting to './data-new'.

    Options:
        --location=<location>   Path on disk where to gently deposit the data
                                    [default: ./data-new]
        -h --help               Show this screen
    """
    for collection in args['<collections>']:
        collection = default_db[collection]
        if collection.count() == 0:
            raise DocoptExit('Collection {!r} is empty'
                             .format(collection.full_name))

        head = Path(args['--location'])/collection.name
        if not head.exists():
            head.mkdir(parents=True)
        for document in collection.find():
            io.YamlManager.dump_record(document, str(head))


@_register
def export(args):
    """Usage: scrapers export [--format=<format>] [--push]

    Options:
        --format=<format>   Export format [default: json]
        -h --help           Show this screen
    """
    repo = pygit2.Repository('data')
    sign = pygit2.Signature('export-script',
                            'c9af0c27360c4d2b@aa0e2d1360b8199c')
    original_ref, export_ref = repo.head.name, 'refs/heads/export'
    if 'export' not in repo.listall_branches():
        repo.create_commit(export_ref, sign, sign, 'Create export branch',
                           repo.TreeBuilder().write(), [])
    repo.checkout(export_ref)
    try:
        for model in (m for m in models.__dict__.values()
                      if tasks._is_subclass(m, models.InsertableRecord)):
            path = Path('data', model.collection.name + '.' + args['--format'])
            print('Exporting {}...'.format(path.name))
            with open(str(path), 'w') as file:
                file.write(model.export(args['--format']))
        repo.index.add_all()
        repo.create_commit(export_ref, sign, sign,
                           'Export data to ' + args['--format'].upper(),
                           repo.index.write_tree(), [repo.head.target])
    finally:
        repo.checkout(original_ref)


@_register('cache--clear')
def clear_text_cache(args):
    """Usage: scrapers cache--clear

    Clear the crawler's cache.

    Options:
        -h --help       Show this screen
    """
    crawling.Crawler.clear_text_cache()


@_register('cache--dump')
def dump_cache(args):
    """Usage: scrapers cache--dump [<location>]

    Dump the crawler's cache at <location>.

    Options:
        -h --help       Show this screen
    """
    crawling.Crawler.dump_cache(args['<location>'])


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
