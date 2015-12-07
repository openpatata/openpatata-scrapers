
"""Aggregate the tasks."""

from importlib import import_module
import itertools
from pathlib import Path

from scrapers.crawling import Task

_isclass = lambda v: isinstance(v, type)
# The built-in `issubclass` considers a class to be its own subclass
# and would return true for `issubclass(Task, Task)`, which we don't want
_issubclass = lambda v, cls: cls in v.__mro__[1:]

# Pull in the globals of all the modules in `pwd`, weed out the non-Tasks
# and assign Tasks to a dictionary of Task.name–Task key–value pairs.
# We're not dealing with Task.name clashes, 'cause that would not not not be
# lazy
TASKS = (import_module('.'.join((__name__, m.stem))).__dict__
         for m in Path(__file__).parent.glob('[!__]*.py'))
TASKS = itertools.chain.from_iterable(i.items() for i in TASKS)
TASKS = {v.name: v for _, v in TASKS if _isclass(v) and _issubclass(v, Task)}
