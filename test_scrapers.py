
import doctest
from functools import reduce
from importlib.machinery import SourceFileLoader
from pathlib import Path
import sys

if __name__ == '__main__':
    failed = reduce(lambda c, m: c+doctest.testmod(m, verbose=True).failed,
                    map(lambda m: SourceFileLoader(*(str(m),)*2).load_module(),
                        Path('./scrapers').glob('*.py')), 0)
    print('Total failures:', failed)

    sys.exit(int(bool(failed)))
