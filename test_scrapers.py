
"""Run `doctest` on all of the package's modules."""

from collections import Counter
import doctest
from functools import reduce
from importlib.machinery import SourceFileLoader
from pathlib import Path
import sys

if __name__ == '__main__':
    tests = reduce(lambda c, m: c + Counter(doctest.testmod(m, verbose=True)
                                                   ._asdict()),
                   map(lambda m: SourceFileLoader(*(str(m),)*2).load_module(),
                       Path('./scrapers').glob('**/*.py')),
                   Counter())
    print('\nTotal attempted tests:', tests['attempted'])
    print('Total failures:', tests['failed'])

    sys.exit(1 if tests['failed'] else 0)
