"""All Celery tasks are declared in submodules of this module."""
from flask_celeryext import FlaskCeleryExt
from ..flask import app
ext = FlaskCeleryExt()
ext.init_app(app)
celery = ext.celery
del app, ext, FlaskCeleryExt

# Use the same URL for both the result backend and the broker.
celery.conf['result_backend'] = celery.conf.broker_url


import importlib  # noqa: E402
import os  # noqa: E402
import pkgutil  # noqa: E402

# Import all submodules of this module
modules = vars()
for _, module, _ in pkgutil.iter_modules([os.path.dirname(__file__)]):
    modules[module] = importlib.import_module('.' + module, __name__)
    del module

# Clean up
del importlib, modules, os, pkgutil
