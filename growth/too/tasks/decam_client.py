import numpy as np
from astropy import time
import astropy.units as u
from astropy.coordinates import SkyCoord
from celery.task import PeriodicTask
from celery.utils.log import get_task_logger

from ..flask import app
from . import celery
from .. import models, decam_db

log = get_task_logger(__name__)

__all__ = ('decam_obs')


def get_obs(start_time, end_time):

    fields = models.Field.query.filter_by(telescope="DECam").all()
    field_ids, ras, decs = [], [], []
    for field in fields:
        field_ids.append(field.field_id)
        ras.append(field.ra)
        decs.append(field.dec)
    field_ids, ras, decs = np.array(field_ids), np.array(ras), np.array(decs)

    catalog1 = SkyCoord(ra=ras*u.degree,
                        dec=decs*u.degree, frame='icrs')

    engine = models.db.get_engine(app, 'DECam')
    decam_db.DBSession.configure(bind=engine)
    results = decam_db.DBSession().query(decam_db.Exposure).filter(
        (decam_db.Exposure.mjd >= start_time.mjd) &
        (decam_db.Exposure.mjd <= end_time.mjd)).all()

    bands = {'g': 1, 'r': 2, 'i': 3, 'z': 4, 'J': 5}
    obstable = []
    for result in results:
        ra, dec = result.ra, result.dec
        obstime = result.mjd
        fid = bands[result.filter]
        exptime = result.header['EXPTIME']

        catalog2 = SkyCoord(ra=ra*u.degree,
                            dec=dec*u.degree,
                            frame='icrs')
        sep = catalog1.separation(catalog2)
        field_id = field_ids[np.argmin(sep)]

        images = decam_db.DBSession().query(decam_db.Image).filter(
            result.id == decam_db.Image.id).all()
        for image in images:
            limmag, seeing = image.lmt_mg, image.seeing
            ccdnum = image.ccdnum

            obstable.append([field_id, result.id, obstime,
                             exptime, fid, limmag, seeing, ccdnum])

    return np.array(obstable)


@celery.task(base=PeriodicTask, shared=False, run_every=3600)
def decam_obs(start_time=None, end_time=None):

    if start_time is None:
        start_time = time.Time.now() - time.TimeDelta(1.0*u.day)
    if end_time is None:
        end_time = time.Time.now()

    obstable = get_obs(start_time, end_time)

    for row in obstable:
        field_id, expid, obstime, exptime, fid, limmag, seeing, ccdnum = row
        obstime = time.Time(obstime, format='mjd').datetime

        models.db.session.merge(
            models.Observation(telescope='DECam', field_id=int(field_id),
                               observation_id=int(expid),
                               obstime=obstime,
                               exposure_time=int(exptime),
                               filter_id=int(fid),
                               seeing=float(seeing),
                               limmag=float(limmag),
                               subfield_id=int(ccdnum),
                               successful=1))

    models.db.session.commit()
