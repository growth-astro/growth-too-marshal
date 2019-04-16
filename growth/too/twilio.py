"""Twilio phone calls."""

from flask import render_template
from flask_twilio import Twilio, Response
from twilio.twiml.voice_response import Say

from .flask import app
from . import models
from . import views  # noqa: F401

twilio = Twilio()
twilio.init_app(app)


@app.route('/event/<datetime:dateobs>/new_voice.twiml')
@twilio.twiml
def event_new_voice(dateobs):
    event = models.Event.query.get_or_404(dateobs)
    response = Response()
    response.append(
        Say(render_template('event_new_voice.txt', event=event)))
    return response


@app.route('/user/test_voice.twiml')
@twilio.twiml
def user_test_voice_twiml():
    response = Response()
    response.append(
        Say(render_template('user_test.txt')))
    return response
