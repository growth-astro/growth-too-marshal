"""Custom filters for Jinja environment."""
import base64
import datetime
import urllib.parse

from .flask import app


@app.template_filter()
def btoa(a):
    """Jinja filter to mimic JavaScript's ``btoa`` function."""
    return base64.b64encode(a.encode()).decode()


@app.template_filter()
def atob(b):
    """Jinja filter to mimic JavaScript's ``atob`` function."""
    return base64.b64decode(b.encode()).decode()


@app.template_filter()
def quote_plus(s):
    return urllib.parse.quote_plus(s)


app.jinja_env.globals['now'] = datetime.datetime.now
