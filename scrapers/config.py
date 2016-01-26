
from .io import YamlManager as _YamlManager

globals().update(_YamlManager.load('config.yaml'))
