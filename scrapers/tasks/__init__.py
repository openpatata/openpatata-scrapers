
"""Aggregate the tasks."""

from importlib import import_module
import itertools as it
from pathlib import Path

from ..crawling import Task


def _is_class(v): return isinstance(v, type)


def _is_subclass(v, cls):
    # The built-in `issubclass` considers a class to be its own subclass
    # and would return True for `issubclass(Task, Task)`, which we don't want
    return cls in v.__mro__[1:]


def _camel_to_snake(s):
    return (''.join(('_' + c if c.isupper() else c) for c in s)
            .strip('_')
            .lower())

# Pull in the globals of all the modules in `pwd`, weed out the non-Tasks
# and assign Tasks to a dictionary of Task.name–Task key–value pairs.
# We're not dealing with Task.name clashes, 'cause that would not not not be
# lazy
TASKS = (import_module('.'.join((__name__, m.stem))).__dict__
         for m in Path(__file__).parent.glob('[!__]*.py'))
TASKS = it.chain.from_iterable(i.items() for i in TASKS)
TASKS = {_camel_to_snake(v.__name__): v
         for _, v in TASKS if _is_class(v) and _is_subclass(v, Task)}
