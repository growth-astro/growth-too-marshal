from getpass import getpass
import os

import click
from flask.cli import FlaskGroup
import lxml.etree
from passlib.apache import HtpasswdFile
from tqdm import tqdm

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
def iers():
    """Update IERS data for precise positional astronomy.

    The IERS Bulletin A data set is used for precise time conversions and
    positional astronomy. To initialize Astroplan, you need to download it.
    According to https://astroplan.readthedocs.io/en/latest/faq/iers.html, you
    need to run this command::

        python -c 'from astroplan import download_IERS_A; download_IERS_A()'

    Unfortunately, the USNO server that provides the data file is extremely
    flaky. This tool attempts to work around that by retrying the download
    several times.
    """
    from retry.api import retry_call
    from astroplan import download_IERS_A
    from urllib.error import URLError

    retry_call(
        download_IERS_A, exceptions=(IndexError, URLError, ValueError),
        tries=5, delay=1, backoff=2)


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

        filenames = ['GRB180116A_Fermi_GBM_Alert.xml',
                     'GRB180116A_Fermi_GBM_Flt_Pos.xml',
                     'GRB180116A_Fermi_GBM_Gnd_Pos.xml',
                     'GRB180116A_Fermi_GBM_Fin_Pos.xml',
                     'MS181101ab-1-Preliminary.xml',
                     'MS181101ab-4-Retraction.xml',
                     'AMON_151115.xml']

        with tqdm(filenames) as progress:
            for filename in progress:
                progress.set_description(
                    'processing GCN {}'.format(filename))
                with app.open_resource(
                        os.path.join('tests/data', filename)) as f:
                    payload = f.read()
                handle(payload, lxml.etree.fromstring(payload))

        tasks.ztf_client.ztf_obs()


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
