
"""Utilities for importing and exporting data files."""

from collections import OrderedDict
import os
from pathlib import Path

import yaml


class DumpError(Exception):
    """Exception raised by `*Manager`s."""


def _represent_str(loader, data):
    # Apply block style to multi-line strings
    return loader.represent_scalar(yaml.resolver.Resolver.DEFAULT_SCALAR_TAG,
                                   data,
                                   '|' if '\n' in data else None)


class _YamlRepresenter(yaml.representer.SafeRepresenter):
    pass

_YamlRepresenter.add_representer(str, _represent_str)


class _YamlDumper(yaml.CSafeDumper, _YamlRepresenter):
    pass


class YamlManager:

    @staticmethod
    def load_record(path: Path):
        """Import a document from disk."""
        with path.open() as file:
            doc = yaml.load(file,
                            Loader=yaml.CSafeLoader)
            return doc

    @staticmethod
    def dump_record(doc, head: Path):
        """Save a database record on disk."""
        try:
            doc_id = doc['_id']
        except KeyError:
            raise DumpError(f'No `_id` in {doc!r}') from None
        with (head/f'{doc_id}.yaml').open('w') as file:
            yaml.dump(doc, file,
                      Dumper=_YamlDumper,
                      allow_unicode=True, default_flow_style=False)
