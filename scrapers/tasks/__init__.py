
"""Aggregate all tasks."""

from importlib import import_module
from pathlib import Path

TASKS = dict(map(
    lambda m: (m.stem, import_module('scrapers.tasks.'+m.stem).HEAD),
    Path(__file__).parent.glob('[!_]*.py')))
