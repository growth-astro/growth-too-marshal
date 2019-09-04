
import sys

from ..flask import app
from ..models import db
from astropy import time
import astropy.units as u
from astropy import table
from astropy.coordinates import SkyCoord
from sqlalchemy import Table, orm
from celery.task import PeriodicTask
# from celery.utils.log import get_task_logger
import numpy as np
import pkg_resources

from . import celery
from .. import models

# log = get_task_logger(__name__)


class DECamTiles(db.Model):
    """ Fetch the DECam tiles from the database."""
    __bind_key__ = 'decam_db'
    __table__ = Table('tiles', db.metadata, autoload=True,
    autoload_with=db.get_engine(app, bind=__bind_key__))


@celery.task(base=PeriodicTask, shared=False, run_every=3600)
def decam_obs(start_time=None, end_time=None):
    """ Periodically query the DECam database to get the properties of the observed tiles."""

    if start_time is None:
        start_time = time.Time.now() - time.TimeDelta(1.0*u.day)
    if end_time is None:
        end_time = time.Time.now()

    # get all the DECam tiles, filter by mjd_obs
    query = db.session.query(DECamTiles).filter(DECamTiles.mjd_obs.between(start_time.mjd, end_time.mjd))

    bands = {'g': 1, 'r': 2, 'i': 3, 'z': 4, 'J': 5, 'U': 6, 'a': 7}
    fields = ( 'exptime', 'ra', 'dec', 'field_id', 'id', 'filter_id', 'subfield_id', 'airmass', 'maglim')

    tiles_table = table.Table(names=fields)

    fields = models.Field.query.filter_by(telescope='DECam')
    field_ids = np.array([field.field_id for field in fields])
    ra = np.array([field.ra for field in fields])
    dec = np.array([field.dec for field in fields])
    tess_catalog = SkyCoord(ra=ra*u.deg, dec=dec*u.deg)

    ra_tiles = np.array([tile.ra for tile in tiles])
    dec_tiles = np.array([tile.dec for tile in tiles])
    tiles_catalog = SkyCoord(ra=ra_tiles*u.deg, dec=dec_tiles*u.deg)    

    idx, d2d, d3d = tiles_catalog.match_to_catalog_sky(tess_catalog)
    for i, tile in enumerate(tiles):
        field_id = field_ids[i]
        filter_id = bands[tile.filter]
        obstime = time.Time(tile.mjd_obs, format='mjd').datetime

        try:
            tiles_table.add_row([round(tile.exptime, 3), tile.ra, tile.dec, int(field_id), \
                int(tile.id), int(filter_id), int(tile.subfield_id), tile.airmass, tile.maglim])
        except AttributeError:
            tiles_table.add_row([round(tile.exptime, 3), tile.ra, tile.dec, int(field_id), \
                int(tile.id), int(filter_id), -1, np.nan, np.nan])            

        for name in zip(tiles_table.colnames):
            if name in ['field_id', 'id', 'filter_id', 'subfield_id'] and not tiles_table[name][i]: tiles_table[name][i] = -1
            elif not tiles_table[name][i]: tiles_table[name][i] = np.nan

        models.db.session.merge(
            models.Observation(telescope='DECam', 
                               field_id=int(tiles_table[i]['field_id']),
                               observation_id=int(tiles_table[i]['id']),
                               obstime=obstime,
                               exposure_time=float(tiles_table[i]['exptime']),
                               filter_id=int(tiles_table[i]['filter_id']),
                               subfield_id=int(tiles_table[i]['subfield_id']),
                               airmass=float(tiles_table[i]['airmass']),
                               limmag=float(tiles_table[i]['maglim']),
                               successful=1))
    models.db.session.commit()
