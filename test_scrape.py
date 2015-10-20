
import doctest
import sys

import scrape

if __name__ == '__main__':
    tests = doctest.testmod(scrape, verbose=True)
    sys.exit(tests.failed)
