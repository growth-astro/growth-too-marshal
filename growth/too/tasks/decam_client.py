
import sys

from ..flask import app
from ..models import db
from astropy import time
import astropy.units as u
from astropy import table
from astropy.coordinates import SkyCoord
from sqlalchemy import Table, orm
from celery.task import PeriodicTask
from celery.utils.log import get_task_logger
from celery.local import PromiseProxy
import numpy as np
import pyvo.dal
import pkg_resources

from . import celery
from .. import models

log = get_task_logger(__name__)



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
    tiles = query.all()

    bands = {'g': 1, 'r': 2, 'i': 3, 'z': 4, 'J': 5, 'U': 6, 'a': 7}
    fields = ( 'exptime', 'ra', 'dec', 'field_id', 'id', 'filter_id', 'subfield_id', 'airmass', 'maglim')

    tiles_table = table.Table(names=fields)

    tessfile = pkg_resources.resource_stream(__name__, '../input/DECam.tess')
    fields = np.recfromtxt(tessfile, usecols=range(3), names=['field_id', 'ra', 'dec'])

    tess_catalog = SkyCoord(ra=fields['ra']*u.deg, dec=fields['dec']*u.deg)

    for i, tile in enumerate(tiles):
        # map the tile ra and dec to the field_id using minimum separation
        coord = SkyCoord(ra=tile.ra*u.deg, dec=tile.dec*u.deg)
        separation = tess_catalog.separation(coord)
        field_id = fields['field_id'][np.argmin(separation)]
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
