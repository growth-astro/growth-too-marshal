"""Tests for the test setup itself."""
import re

from .. import tasks
from ..flask import app
from ..models import db


def test_celery(celery):
    """Check that we are using the test configuration for Celery."""
    assert tasks.celery.conf['task_always_eager']


def test_flask(flask):
    """Check that we are using the test configuration for Flask."""
    assert app.testing


def test_mail(mail):
    """Check that we are using the test configuration for Flask."""
    assert tasks.email.mail.suppress


def test_sqlalchemy(database):
    """Check that we are using the test configuration for SQLAlchemy.
    If this test fails, then there is a danger that we might accidentally
    modify the production database!"""
    assert re.match(r'^postgresql://postgres@127\.0\.0\.1:\d+$',
                    str(db.engine.url)) is not None
