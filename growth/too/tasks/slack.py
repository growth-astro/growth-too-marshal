#
# Copyright (C) 2019 Michael Coughlin
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
# Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
"""
Some tools for parsing and composing slack messages.
"""
__author__ = "Michael Coughlin <michael.coughlin@ligo.org>"

from flask import render_template
from slack import WebClient
from celery.local import PromiseProxy

from ..flask import app
from . import celery
from .. import models


def get_client():
    return WebClient(token=app.config['SLACK_API_TOKEN'])


client = PromiseProxy(get_client)


@celery.task(ignore_result=True, shared=False)
def slack_too(telescope, queue_name):
    body = render_template('too.email', telescope=telescope,
                           queue_name=queue_name)

    response = client.chat_postMessage(
        channel='#general',
        username='Grobot',
        as_user=False,
        icon_url='https://vignette.wikia.nocookie.net/mario/images/9/99/Grobot.png/revision/latest?cb=20130903151824',  # noqa: E501
        text=body)

    if not response["ok"]:
        raise RuntimeError('Slack ToO message failed...', response)


@celery.task(ignore_result=True, shared=False)
def slack_everyone(dateobs):
    event = models.Event.query.get(dateobs)

    body = render_template('event_new.email', event=event)

    response = client.chat_postMessage(
        channel='#general',
        username='Grobot',
        as_user=False,
        icon_url='https://vignette.wikia.nocookie.net/mario/images/9/99/Grobot.png/revision/latest?cb=20130903151824',  # noqa: E501
        text=body)

    if not response["ok"]:
        raise RuntimeError('Slack event message failed...', response)
