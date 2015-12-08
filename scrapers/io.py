
"""Utilities for importing and exporting data files."""

from collections import OrderedDict
import os

import yaml


class DumpError(Exception):
    """Exception raised by `*Manager`s."""


class _YamlConstructor(yaml.constructor.SafeConstructor):

    def construct_mapping(self, node, deep=False):
        """Alpha-sort !maps on import."""
        val = super().construct_mapping(node, deep)
        return OrderedDict(sorted(val.items()))

_YamlConstructor.add_constructor(yaml.resolver.Resolver.DEFAULT_MAPPING_TAG,
                                 _YamlConstructor.construct_mapping)


class _YamlLoader(yaml.CSafeLoader, _YamlConstructor):
    """Inject our `_YamlConstructor` before `yaml.CSafeLoader`'s."""


class _YamlRepresenter(yaml.representer.SafeRepresenter):

    def represent_str(self, data):
        """Apply block style to multi-line strings."""
        style = '|' if '\n' in data else None
        return self.represent_scalar(yaml.resolver.Resolver.DEFAULT_SCALAR_TAG,
                                     data, style)

_YamlRepresenter.add_representer(str,
                                 _YamlRepresenter.represent_str)
_YamlRepresenter.add_representer(OrderedDict,
                                 # PyYAML sorts dicts on export out of the box
                                 _YamlRepresenter.represent_dict)


class _YamlDumper(yaml.CSafeDumper, _YamlRepresenter):
    """Inject our `_YamlRepresenter` before `CSafeDumper`'s."""


class YamlManager:

    @staticmethod
    def load(path):
        """Import a document from disk."""
        with open(path) as file:
            doc = yaml.load(file.read(),
                            Loader=_YamlLoader)
            return doc

    @classmethod
    def load_record(cls, path, filename):
        """Import a database record from disk."""
        doc = cls.load(path)
        doc['_filename'] = filename
        return doc

    @staticmethod
    def dump_record(doc, head):
        """Save a database record to disk."""
        try:
            path = os.path.join(head, doc.pop('_filename')) + '.yaml'
            del doc['_id']
        except KeyError:
            raise DumpError('Unable to extract filename from record with'
                            ' `_id` {!r}'.format(doc['_id'])) from None
        with open(path, 'w') as file:
            yaml.dump(doc, file,
                      Dumper=_YamlDumper,
                      allow_unicode=True, default_flow_style=False)
