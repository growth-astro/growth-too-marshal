import pytest
from pytest_socket import socket_allow_hosts

from .. import tasks
from ..flask import app


@pytest.fixture(autouse=True, scope='session')
def database(postgresql_proc):
    """Use a disposible Postgresql database for all tests."""
    database_uri = 'postgresql://postgres@{proc.host}:{proc.port}'.format(
        proc=postgresql_proc)
    app.config['SQLALCHEMY_DATABASE_URI'] = database_uri
    from .. import models
    models.create_all()
    models.db.session.commit()


@pytest.fixture
def flask(monkeypatch):
    """Set the Flask TESTING flag."""
    monkeypatch.setitem(app.config, 'TESTING', True)
    monkeypatch.setitem(app.config, 'SERVER_NAME', 'example.edu')
    yield app.test_client()


@pytest.fixture
def celery(monkeypatch):
    """Use testing configuration for Celery."""
    monkeypatch.setitem(tasks.celery.conf, 'task_always_eager', True)
    monkeypatch.setitem(tasks.celery.conf, 'task_eager_propagates', True)


@pytest.fixture
def mail(monkeypatch):
    """Set the Flask-Mail MAIL_SUPPRESS_SEND flag."""
    monkeypatch.setattr(tasks.email.mail.state, 'suppress', True)


def pytest_runtest_setup():
    socket_allow_hosts(['127.0.0.1'])
