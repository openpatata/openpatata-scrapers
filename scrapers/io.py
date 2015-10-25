
"""Utilities for importing and exporting data files."""

from collections import OrderedDict
import os

import yaml
from yaml.resolver import BaseResolver


class _YamlLoader(yaml.CLoader):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_constructor(BaseResolver.DEFAULT_MAPPING_TAG,
                             self.construct_mapping)

    def construct_mapping(self, _, node, deep=False):
        """Maintain the order of !maps on import."""
        return OrderedDict((self.construct_object(k, deep=deep),
                            self.construct_object(v, deep=deep))
                           for k, v in node.value)


class _YamlDumper(yaml.CDumper):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_representer(str, self.represent_scalar)

    def represent_scalar(self, _, value, style=None):
        """Apply block style to multi-line strings."""
        style = '|' if '\n' in value else None
        return super().represent_scalar(BaseResolver.DEFAULT_SCALAR_TAG,
                                        value, style)


class YamlManager:

    @staticmethod
    def load(path, filename):
        """Import a document from disk."""
        with open(path) as file:
            doc = yaml.load(file.read(),
                            Loader=_YamlLoader)
            doc['_filename'] = filename
            return doc

    @staticmethod
    def dump(doc, head):
        """Save a document to disk."""
        path = os.path.join(head, doc.pop('_filename'))
        with open(path, 'w') as file:
            yaml.dump(doc, file,
                      Dumper=_YamlDumper,
                      allow_unicode=True, default_flow_style=False)
