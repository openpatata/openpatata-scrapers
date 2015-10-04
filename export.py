
import os

import yaml

# Block style for multi-line strings
yaml.CDumper.add_representer(
    str, lambda dumper, value:
        dumper.represent_scalar('tag:yaml.org,2002:str', value,
                                style='|' if '\n' in value else None))


def yaml_dump(data, path):
    """Save a document to disk as YAML."""
    head = os.path.dirname(path)
    if not os.path.exists(head):
        os.makedirs(head)
    with open(path, 'w') as file:
        yaml.dump(data, file, Dumper=yaml.CDumper,
                  allow_unicode=True, default_flow_style=False)
