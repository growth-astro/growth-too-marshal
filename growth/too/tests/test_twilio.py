import datetime
from unittest import mock

import pytz

from .. import models
from .. import tasks


def mock_now_utc():
    return datetime.datetime(2018, 2, 22, 2, 42, 52, 954222, tzinfo=pytz.UTC)


def test_now_utc():
    # We mock up now_utc in the test below, so we should test the real version
    # here ;-)
    now1 = datetime.datetime.now(pytz.utc)
    now2 = tasks.twilio.now_utc()
    assert now2.tzinfo == now1.tzinfo
    assert now2 >= now1


@mock.patch('growth.too.tasks.twilio.twilio.call_for')
@mock.patch('growth.too.tasks.twilio.now_utc', mock_now_utc)
def test_call_everyone(mock_call_for, celery, database):
    # Fritz Zwicky does not have a cell phone. He will not get phone alerts.
    fritz = models.User(name='fritz')
    models.db.session.add(fritz)

    # Shri Kulkarni has a cell phone. He is on an Artic leture tour and
    # receives alerts during normal business hours.
    shri = models.User(
        name='shri', phone='+15555551212', timezone='Arctic/Longyearbyen',
        alert_from='09:00', alert_to='17:00')
    models.db.session.add(shri)

    # Michael Coughlin is in Pasadena and works night and day; he gets alerts
    # at all times except from the hours of 17:00 to 23:00.
    michael = models.User(
        name='michael', phone='+15555551213', timezone='America/Los_Angeles',
        voice=True, alert_from='23:00', alert_to='17:00')
    models.db.session.add(michael)

    # Leo Singer is in Washington, DC, and gets voice alerts at all times!
    leo = models.User(
        name='leo', phone='+15555551214', timezone='America/New_York',
        voice=True)
    models.db.session.add(leo)

    # Egads! A gamma-ray burst!
    event = models.Event(
        dateobs='2018-02-22T02:39:27', tags=['Fermi', 'Short', 'GRB'])
    models.db.session.add(event)
    models.db.session.commit()

    # Everybody wake up and panic!!!
    tasks.twilio.call_everyone('event_new_voice', dateobs=event.dateobs)

    # Now check that we woke up the right people.
    mock_call_for.assert_called_once_with(
        'event_new_voice', leo.phone.e164, dateobs=event.dateobs)
