
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

def check_column(query_list, colname):
    for col in query_list:
        if col['name'] == str(colname):
            colname = col['expr']
            break
    return colname


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
    field_ids = tess_catalog[idx]

    for i, row in enumerate(query):
        field_id, id, filter_id, subfield_id, exptime, mjd_obs, airmass, maglim = [-1, -1, -1, -1, np.nan, np.nan, np.nan, np.nan]
        colnames = [id, subfield_id, exptime, mjd_obs, airmass, maglim]
        row_list = row.column_descriptions
        for colname in colnames:
            colname = check_column(row_list, colname)
        try:
            field_id = field_ids[i]
            filter_id = bands[row.filter]
            mjd_obs = time.Time(mjd_obs, format='mjd').datetime
            exptime = float(round(exptime, 3))
        except ValueError: paßss
        # try:
        #     tiles_table.add_row([round(tile.exptime, 3), tile.ra, tile.dec, int(field_id), \
        #         int(tile.id), int(filter_id), int(tile.subfield_id), tile.airmass, tile.maglim])
        # except AttributeError:
        #     tiles_table.add_row([round(tile.exptime, 3), tile.ra, tile.dec, int(field_id), \
        #         int(tile.id), int(filter_id), -1, np.nan, np.nan])            

        models.db.session.merge(
            models.Observation(telescope='DECam', 
                               field_id=int(field_id),
                               observation_id=int(id),
                               obstime=obstime,
                               exposure_time=float(exptime),
                               filter_id=int(filter_id),
                               subfield_id=int(subfield_id),
                               airmass=float(airmass),
                               limmag=float(maglim),
                               successful=1))
    models.db.session.commit()
