
import os as _os

DB = _os.environ.get('OPENPATATA_SCRAPERS_DB',
                     'mongodb://localhost:27017/openpatata-data')
CACHE_DB = _os.environ.get('OPENPATATA_SCRAPERS_CACHE_DB',
                           'mongodb://localhost:27017/openpatata-data-cache')
SCHEMAS = _os.environ.get('OPENPATATA_SCRAPERS_SCHEMAS',
                          './data/_schemas')
