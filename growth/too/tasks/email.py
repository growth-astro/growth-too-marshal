#
# Copyright (C) 2015  Leo Singer
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
Some tools for parsing and composing e-mails.
"""
__author__ = "Leo Singer <leo.singer@ligo.org>"


from flask_mail import Mail, Message
from flask import render_template
import email
import email.encoders
import email.mime.text
import email.utils
import email.iterators

from ..flask import app
from . import celery
from .. import models
from .twilio import now_utc, user_is_on_duty

mail = Mail(app)
send = mail.send
record_messages = mail.record_messages


def get_body_text(message):
    """Get the text parts of a message as one big string, with carriage returns
    and/or linefeeds normalized to linefeeds."""
    return '\n'.join(part.get_payload() for part in
                     email.iterators.typed_subpart_iterator(message))


def append_text_plain(message, text):
    if message.is_multipart():
        last_part = message.get_payload(len(message.get_payload()) - 1)
    else:
        last_part = message
    if last_part.get_content_type() == "text/plain":
        charset = last_part.get_charset()
        new_text = last_part.get_payload(decode=True) + text
        last_part.set_payload(charset.body_encode(new_text))
    else:
        message.attach(email.mime.text.MIMEText(text, 'plain'))


class ReplyMessage(Message):
    def __init__(self, original, **kwargs):
        extra_headers = kwargs.setdefault('extra_headers', {})
        extra_headers["In-Reply-To"] = original["Message-ID"]
        extra_headers["References"] = original["Message-ID"]
        if original["Subject"].startswith("Re: "):
            kwargs["subject"] = original["Subject"]
        else:
            kwargs["subject"] = "Re: " + original["Subject"]
        kwargs["body"] = (
            kwargs.get("body", "") + "\n" + render_template(
                "reply.email",
                original_date=original["Date"],
                original_from=original["From"],
                original_body=get_body_text(original)
            )
        )
        super(ReplyMessage, self).__init__(**kwargs)


@celery.task(ignore_result=True, shared=False)
def compose_too(telescope, queue_name):
    body = render_template('too.email', telescope=telescope,
                           queue_name=queue_name)

    message = Message(
        subject='Re: {0}-{1}'.format(telescope, queue_name),
        recipients=[app.config['EMAIL_TOO']],
        cc=[app.config.get('REPLY_TO') or app.config['MAIL_DEFAULT_SENDER']],
        body=body,
        sender=app.config['EMAIL_TOO']
    )

    send(message)


@celery.task(ignore_result=True, shared=False)
def email_everyone(dateobs):
    event = models.Event.query.get(dateobs)
    now = now_utc()
    emails = []
    for user in models.User.query.filter(models.User.email.isnot(None)):
        if user_is_on_duty(now, user):
            emails.append(user.email)

    body = render_template('event_new.email', event=event)

    message = Message(
        subject='Re: {0}'.format(event.dateobs),
        recipients=[app.config['EMAIL_TOO']],
        cc=emails,
        body=body,
        sender=app.config['EMAIL_TOO']
    )

    send(message)
