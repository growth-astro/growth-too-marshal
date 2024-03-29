from unittest.mock import MagicMock
from unittest.mock import create_autospec

import pytest
from pytest_socket import socket_allow_hosts

from celery.local import PromiseProxy

from .. import tasks
from ..flask import app


def uri_for_proc(proc):
    return f'postgresql://{proc.user}:{proc.password}@{proc.host}:{proc.port}'


@pytest.fixture(autouse=True, scope='session')
def database(postgresql_proc):
    """Use a disposible Postgresql database for all tests."""
    socket_allow_hosts([postgresql_proc.host])
    database_uri = uri_for_proc(postgresql_proc)
    app.config['SQLALCHEMY_DATABASE_URI'] = database_uri
    for key in app.config['SQLALCHEMY_BINDS']:
        app.config['SQLALCHEMY_BINDS'][key] = database_uri
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


@pytest.fixture(autouse=True)
def slackclient(monkeypatch):
    client = create_autospec(PromiseProxy)
    client.chat_postMessage = {"ok": True}
    monkeypatch.setattr(tasks.slack, 'client', MagicMock(client))
