
import csv
import itertools as it
from pathlib import Path

import jellyfish

from .models import MP, MultilingualField
from .text_utils import translit_elGrek2Latn, translit_el2tr


def pair_name(name, names_and_ids, existing_names):
    if name in existing_names:
        return name, existing_names[name]
    options = ((jellyfish.jaro_distance(name.lower(), new_name.lower()), id_)
               for id_, new_name in names_and_ids.items())
    options = tuple(enumerate(sorted(options, reverse=True)[:5]))
    selection = ''
    try:
        _, (_, selection) = options[int(input('''\

Select one of the following for {name!r}.
Press Enter to select the first option and ^C and Enter to skip or
^C again to exit.

{options}
'''.format(name=name, options='\n'.join(map(repr, options)))) or 0)]
    except KeyboardInterrupt:
        if input('''\

Create record?  [y/N]
''') == 'y':
            mp = MP(name=MultilingualField(el=name,
                                           en=translit_elGrek2Latn(name),
                                           tr=translit_el2tr(name)))
            mp.insert()
            selection = mp._id
    return name, selection


def load_pairings(filename):
    with open(Path(__file__).parent/'data'/'reconciliation'/filename) as file:
        return dict(it.islice(csv.reader(file), 1, None))
