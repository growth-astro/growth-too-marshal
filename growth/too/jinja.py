"""Custom filters for Jinja environment."""

import re
import textwrap
import base64
import datetime
import urllib.parse

import jinja2

from .flask import app

__all__ = ('env',)

env = jinja2.Environment(
    loader=jinja2.PackageLoader(__name__, 'templates'),
    # The options below are suggested by the Chromium Jinja style guide,
    # https://www.chromium.org/developers/jinja.
    keep_trailing_newline=True,  # newline-terminate generated files
    lstrip_blocks=True,  # so can indent control flow tags
    trim_blocks=True  # so don't need {%- -%} everywhere
)

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


env.filters['rewrap'] = rewrap

@app.template_filter()
def quote_plus(s):
    return urllib.parse.quote_plus(s)


app.jinja_env.globals['now'] = datetime.datetime.now
