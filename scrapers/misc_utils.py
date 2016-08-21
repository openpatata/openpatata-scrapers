
"""Miscellaneous utilities."""

import itertools


def starfilter(function, iterable):
    """A filtering equivalent to `itertools.starmap`.

    >>> from string import ascii_lowercase
    >>> tuple(starfilter(lambda index, _: index % 2 == 0,
    ...                  enumerate(ascii_lowercase, start=1)))   # doctest: +ELLIPSIS
    ((2, 'b'), (4, 'd'), (6, 'f'), ...)
    """
    i1, i2 = itertools.tee(iter(iterable))
    return itertools.compress(i2, itertools.starmap(function, i1))


def is_subclass(v, cls):
    """Check `v` is derived from `cls`.

    The built-in `issubclass` considers a class to be its own subclass
    and would return True for `issubclass(Task, Task)`, which we don't want.
    """
    return isinstance(v, type) and cls in v.__mro__[1:]
