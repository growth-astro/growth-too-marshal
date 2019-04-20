"""Custom filters for Jinja environment."""

import re
import textwrap
import base64
import datetime
import urllib.parse

import jinja2

from .flask import app


@app.template_filter()
def btoa(a):
    """Jinja filter to mimic JavaScript's ``btoa`` function."""
    return base64.b64encode(a.encode()).decode()


@app.template_filter()
def atob(b):
    """Jinja filter to mimic JavaScript's ``atob`` function."""
    return base64.b64decode(b.encode()).decode()

_rewrap_regex = re.compile(
    r'^[ \t]+.+\n|([^ \t\n]+[^\n]*\n)+|.*',
    flags=re.MULTILINE)


@app.template_filter()
def rewrap(text):
    """Rewrap the given text, paragraph by paragraph. Treat any line that begins
    with whitespace as a separate paragraph, and any consecutive sequence of
    lines that do not begin with whitespace as a paragraph."""
    return '\n'.join(textwrap.fill(text[match.start():match.end()])
                     for match in _rewrap_regex.finditer(text))


app.jinja_env.filters['rewrap'] = rewrap

@app.template_filter()
def quote_plus(s):
    return urllib.parse.quote_plus(s)


app.jinja_env.globals['now'] = datetime.datetime.now
