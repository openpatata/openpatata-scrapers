
"""Aggregate the tasks."""

from importlib import import_module
import itertools as it
from pathlib import Path

from ..crawling import Task


def _is_subclass(v, cls):
    # The built-in `issubclass` considers a class to be its own subclass
    # and would return True for `issubclass(Task, Task)`, which we don't want
    return isinstance(v, type) and cls in v.__mro__[1:]


def _camel_to_snake(s):
    name = ''.join(('_' if c is True else '') + ''.join(t)
                   for c, t in it.groupby(s, key=lambda i: i.isupper()))
    name = name.lower().strip('_')
    return name

# Pull in the globals of all the modules in `pwd`, weed out the non-Tasks
# and assign Tasks to a dictionary of Task.name–Task key–value pairs.
# We're not dealing with Task.name clashes, 'cause that would not not not be
# lazy
TASKS = (import_module('.'.join((__name__, m.stem))).__dict__
         for m in Path(__file__).parent.glob('[!__]*.py'))
TASKS = it.chain.from_iterable(i.items() for i in TASKS)
TASKS = {_camel_to_snake(v.__name__): v
         for _, v in TASKS if _is_subclass(v, Task)}
