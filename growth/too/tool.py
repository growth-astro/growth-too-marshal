from getpass import getpass
import os

import click
from flask.cli import FlaskGroup
import lxml.etree
from passlib.apache import HtpasswdFile
import pkg_resources

from .flask import app
from . import models, tasks
from . import views, twilio  # noqa: F401


@click.group(cls=FlaskGroup, create_app=lambda *args, **kwargs: app)
def main():
    """Command line management console for the GROWTH ToO Marshal"""


@app.cli.command(context_settings=dict(allow_extra_args=True,
                                       ignore_unknown_options=True))
@click.pass_context
def celery(ctx):
    """Manage Celery cluster."""
    tasks.celery.start(['celery'] + ctx.args)


@app.cli.command()
def gcn():
    """Listen for GCN Notices."""
    from .gcn import listen
    listen()


@app.cli.command()
@click.argument('username', required=False)
def passwd(username):
    """Set the password for a user."""
    if username is None:
        username = input('Username: ')
    password = getpass()

    path = os.path.join(app.instance_path, 'htpasswd')
    os.makedirs(app.instance_path, exist_ok=True)
    try:
        htpasswd = HtpasswdFile(path)
    except FileNotFoundError:
        htpasswd = HtpasswdFile()

    htpasswd.set_password(username, password)
    htpasswd.save(path)


@app.cli.group()
def db():
    """Manage the PostgreSQL database."""


@db.command()
@click.option('--sample', is_flag=True, help="Populate with sample data.")
def create(sample):
    """Create all tables from SQLAlchemy models"""
    models.create_all()
    models.db.session.commit()

    if sample:
        from .gcn import handle

        # Don't rely on Celery to be functional.
        tasks.celery.conf['task_always_eager'] = True
        models.db.session.merge(models.User(name='fritz'))
        models.db.session.commit()

        for filename in ['tests/data/GRB180116A_Fermi_GBM_Alert.xml',
                         'tests/data/GRB180116A_Fermi_GBM_Flt_Pos.xml',
                         'tests/data/GRB180116A_Fermi_GBM_Gnd_Pos.xml',
                         'tests/data/GRB180116A_Fermi_GBM_Fin_Pos.xml',
                         'tests/data/MS181101ab-1-Preliminary.xml',
                         'tests/data/MS181101ab-4-Retraction.xml',
                         'tests/data/AMON_151115.xml']:
            payload = pkg_resources.resource_string(
                __name__, filename)
            handle(payload, lxml.etree.fromstring(payload))

        tasks.obs.ztf_obs()


@db.command()
@click.option('--preserve', help='Preserve the named table.', multiple=True)
def drop(preserve):
    """Drop all tables from SQLAlchemy models"""
    models.db.reflect(bind=None)
    models.db.metadata.drop_all(
        bind=models.db.get_engine(app, bind=None),
        tables=[value for key, value in models.db.metadata.tables.items()
                if key not in preserve])
    models.db.session.commit()


@db.command()
@click.option('--sample', is_flag=True, help="Populate with sample data.")
@click.pass_context
def recreate(ctx, sample):
    """Drop and recreate all tables from SQLAlchemy models"""
    ctx.invoke(drop)
    ctx.forward(create)
