import datetime

import pytz

from ..twilio import twilio
from . import celery
from .. import models


def int_time(t):
    return (
        t.microsecond + 1_000_000 * (t.second + 60 * (t.minute + 60 * t.hour)))


def time_in_interval(x, a, b):
    n = 86_400_000_000
    x = int_time(x)
    a = int_time(a)
    b = int_time(b)
    return (x - a) % n <= (b - a) % n


def now_utc():
    return datetime.datetime.now(pytz.utc)


def user_is_on_duty(now, user):
    """Determine if a user is on duty at a given time."""
    if user.alert_from is not None and user.alert_to is not None:
        tz = pytz.timezone(user.timezone)
        time = now.astimezone(tz).time()
        if not time_in_interval(time, user.alert_from, user.alert_to):
            return False
    return True


@celery.task(ignore_result=True, shared=False)
def call_for(endpoint, phone, **values):
    twilio.call_for(endpoint, phone, **values)


@celery.task(ignore_result=True, shared=False)
def text_for(body, phone, **values):
    twilio.message(body, phone, **values)


@celery.task(ignore_result=True, shared=False)
def text_everyone(body, **values):
    now = now_utc()
    for user in models.User.query.filter(models.User.phone.isnot(None)):
        if user_is_on_duty(now, user):
            text_for.s(body, user.phone.e164, **values).delay()


@celery.task(ignore_result=True, shared=False)
def call_everyone(endpoint, **values):
    now = now_utc()
    for user in models.User.query.filter(models.User.phone.isnot(None)) \
            .filter(models.User.voice):
        if user_is_on_duty(now, user):
            call_for.s(endpoint, user.phone.e164, **values).delay()
