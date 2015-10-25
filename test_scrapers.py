
import doctest
from functools import reduce
import sys

from scrapers import records, text_utils

if __name__ == '__main__':
    failed = reduce(lambda c, m: c+doctest.testmod(m, verbose=True).failed,
                    (records, text_utils), 0)
    print('Total failures:', failed)
    sys.exit(int(bool(failed)))
