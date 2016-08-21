
"""\
Cypriot-parliament scraper.

This package collects and stratifies some of what's made available on
the Cypriot parliament's website.  For more information, please see the
README.

Usage: scrapers [-v] <command> [<args> ...]

Commands:
    clear-cache    Clear the crawler's cache
    dump           Dump documents in a collection as YAML
    dump-cache     Dump the crawler's cache on disk
    export         Export a collection to CSV or JSON
    init           Populate the database
    run            Run a scraping task

Options:
    -h --help       Show this screen
    -v --verbose    Print error messages of all levels
"""

import json
import logging
from pathlib import Path
import subprocess
import textwrap

from docopt import docopt, DocoptExit

from . import crawling, default_db, io, models, tasks
from .misc_utils import is_subclass


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
        default_db[collection].insert_one(io.YamlManager.load_record(str(path)))


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
    """Usage: scrapers export

    Export the database as a JSON data package.

    Options:
        -h --help           Show this screen
    """
    def ǀ(cmd):
        return subprocess.run('git -C data ' + cmd,
                              check=True, shell=True, stdout=subprocess.PIPE)

    assert ǀ('rev-parse --abbrev-ref HEAD').stdout.strip() == b'master'
    has_stash = ǀ('stash').stdout.strip() != b'No local changes to save'
    ǀ('checkout export')

    for _, model in models.registry:
        print('Exporting {}...'.format(model.collection.name))
        with Path('data', model.collection.name + '.json').open('w') as file:
            file.write(model.export(format='json'))
    with Path('data', 'datapackage.json').open('w') as file:
        json.dump(models.registry.create_data_package(), file, indent=2)

    ǀ('add .')
    ǀ('commit --author "export-script <export@script>"'
      '       --message "Export data to JSON"')
    ǀ('checkout master')
    if has_stash:
        ǀ('stash pop')


@_register('clear-cache')
def clear_cache(args):
    """Usage: scrapers clear-cache

    Clear the crawler's cache.

    Options:
        -h --help       Show this screen
    """
    crawling.Crawler.clear_text_cache()


@_register('dump-cache')
def dump_cache(args):
    """Usage: scrapers dump-cache [<location>]

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
                         '; '.join(sorted(_register.__dict__))) from None
    else:
        args = docopt(command.__doc__,
                      argv=[args['<command>']] + args['<args>'])
        command(args)

if __name__ == '__main__':
    main()
