import pytest

from .. import tasks
from ..flask import app


@pytest.fixture(autouse=True, scope='session')
def temp_database(postgresql_proc):
    """Use a disposible Postgresql database for all tests."""
    database_uri = 'postgresql://postgres@{proc.host}:{proc.port}'.format(
        proc=postgresql_proc)
    app.config['SQLALCHEMY_DATABASE_URI'] = database_uri
    for key in app.config['SQLALCHEMY_BINDS']:
        app.config['SQLALCHEMY_BINDS'][key] = database_uri


@pytest.fixture
def database():
    """Start from an empty database."""
    from .. import models
    models.create_all()
    yield
    models.db.session.commit()
    models.db.drop_all()


@pytest.fixture
def flask(monkeypatch):
    """Set the Flask TESTING flag."""
    monkeypatch.setitem(app.config, 'TESTING', True)
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
