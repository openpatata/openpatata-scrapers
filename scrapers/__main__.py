
"""\
Cypriot-parliament scraper.

Usage: scrapers [-v] <command> [<args> ...]

Commands:
    cache     Manage the client cache
    data      Manage the scraper data
    tasks     Run a scraping task

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

from . import client, default_db, io, models, records, tasks


def _git(cmd):
    return subprocess.run('git -C data ' + cmd,
                          check=True, shell=True, stdout=subprocess.PIPE)


def _register(fn, name=None):
    if isinstance(fn, str):
        return lambda v: _register(v, name=fn)
    fn.__doc__ = fn.__doc__.partition('\n')
    fn.__doc__ = fn.__doc__[:-1] + (textwrap.dedent(fn.__doc__[-1]),)
    fn.__doc__ = ''.join(fn.__doc__)
    _register.__dict__[name or fn.__name__] = fn
    return fn


def _exec_command(args, *, subcommand=None):
    try:
        command = _register.__dict__[
            ' '.join(i for i in [subcommand, args['<command>']] if i)]
    except KeyError:
        raise DocoptExit('Unknown command')
    else:
        args = docopt(command.__doc__,
                      argv=([i for i in [subcommand, args['<command>']] if i] +
                            args['<args>']),
                      options_first=not subcommand)
        command(args)


@_register('data')
def manage_data(args):
    """Usage: scrapers data <command> [<args> ...]

    Commands:
        load      Populate the database
        unload    Dump documents in a collection as YAML
        export    Export the database as a JSON data package

    Options:
        -h --help       Show this screen
    """
    _exec_command(args, subcommand='data')


@_register('data load')
def load_data(args):
    """Usage: scrapers data load [--keep-db] [<from-folders> ...]

    Populate the database <from-folders>, defaulting to `./data/mps`.

    Options:
        -k --keep-db    Don't drop the database before importing
        -h --help       Show this screen
    """
    assert _git('rev-parse --abbrev-ref HEAD').stdout.strip() == b'master'
    if not args['--keep-db']:
        default_db.command('dropDatabase')

    files = ((f, d.stem)
             for d in map(Path, args['<from-folders>'] or ('./data/mps',))
             for f in d.glob('*.yaml'))
    for path, collection in files:
        default_db[collection].insert_one(io.YamlManager.load_record(path))


@_register('data unload')
def unload_data(args):
    """Usage: scrapers data unload [--location=<location>] [<collections> ...]

    Dump <collections> at <location>.  The default behaviour is to dump all
    collections.

    Options:
        --location=<location>   Path on disk to dump the data  [default: ./data-new]
        -h --help               Show this screen
    """
    for collection in (args['<collections>'] or default_db.collection_names()):
        collection = default_db[collection]
        if collection.count() == 0:
            raise DocoptExit(f'Collection {collection.full_name!r} is empty')
        print(f'Unloading {collection.name!r}...')

        head = Path(args['--location'])/collection.name
        if not head.exists():
            head.mkdir(parents=True)
        for document in collection.find():
            io.YamlManager.dump_record(document, head)


@_register('data export')
def export_data(args):
    """Usage: scrapers data export [-p] [-s]

    Export the database as a JSON data package.

    Options:
        -p --push           Push changes to remote repo
        -s --stay           Stay on export branch
        -h --help           Show this screen
    """
    assert _git('rev-parse --abbrev-ref HEAD').stdout.strip() == b'master'
    has_stash = _git('stash').stdout.strip() != b'No local changes to save'
    _git('checkout export')

    for _, model in records.InsertableRecord.__records__:
        print(f'Exporting {model.collection.name!r}...')
        with Path('data', model.collection.name + '.json').open('w') as file:
            file.write(model.export(format='json'))
    with Path('data', 'datapackage.json').open('w') as file:
        json.dump(records.InsertableRecord.__records__.create_data_package(),
                  file, indent=2)

    _git('add .')
    _git('commit --author "export-script <export@script>"'
         '       --message "Export data to JSON"')
    if args['--push']:
        _git('push')
    if not args['--stay']:
        _git('checkout master')
        if has_stash:
            _git('stash pop')


@_register('tasks')
def run_task(args):
    """Usage: scrapers tasks run [-d] <task>

    Options:
        -d --debug      Print `asyncio` debugging messages to `stderr`
        -h --help       Show this screen
    """
    if args['<task>'] not in tasks.TASKS:
        raise DocoptExit('Available tasks are: ' +
                         '\n'.join(' ' * len('Available tasks are: ') + i
                                   for i in sorted(tasks.TASKS)).strip())
    client.Client(debug=args['--debug'])(tasks.TASKS[args['<task>']])


@_register('cache')
def manage_cache(args):
    """\
    Usage: scrapers cache clear
           scrapers cache dump [<location>]

    Options:
        -h --help       Show this screen
    """
    if args['clear']:
        client.Client.clear_text_cache()
    elif args['dump']:
        client.dump_cache(args['<location>'])


def main():
    args = docopt(__doc__, options_first=True)
    if args['--verbose']:
        logging.basicConfig(level=logging.DEBUG)
    _exec_command(args)

if __name__ == '__main__':
    main()
