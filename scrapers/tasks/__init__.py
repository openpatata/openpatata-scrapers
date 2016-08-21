
"""Aggregate the tasks."""

from importlib import import_module
import itertools as it
from pathlib import Path

from ..crawling import Task
from ..misc_utils import is_subclass


def _camel_to_snake(s):
    name = ''.join(('_' if c is True else '') + ''.join(t)
                   for c, t in it.groupby(s, key=lambda i: i.isupper()))
    name = name.lower().strip('_')
    return name

# Pull in the globals of all the modules in `pwd`, weed out the non-Tasks
# and assign Tasks to a dictionary of Task.name–Task key–value pairs.
# We're not dealing with Task.name clashes, 'cause that would not not not be
# lazy
TASKS = (import_module('.'.join((__name__, m.stem)))
         for m in Path(__file__).parent.glob('[!__]*.py'))
TASKS = it.chain.from_iterable(zip(it.repeat(i.__name__.split('.')[-1]),
                                   i.__dict__.values())
                               for i in TASKS)
TASKS = {':'.join((m, _camel_to_snake(v.__name__))): v
         for m, v in TASKS if is_subclass(v, Task)}
