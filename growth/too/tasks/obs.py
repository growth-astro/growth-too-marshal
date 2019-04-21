import numpy as np
from astropy import time
import astropy.units as u
from pyvo.dal import TAPService
from celery.task import PeriodicTask
from celery.utils.log import get_task_logger

from . import celery
from .. import models

log = get_task_logger(__name__)

__all__ = ('ztf_obs',)


def get_tap_client():
    url = 'https://irsa.ipac.caltech.edu/TAP'
    return TAPService(url)


@celery.task(base=PeriodicTask, shared=False, run_every=3600)
def ztf_obs(start_time=None, end_time=None):

    if start_time is None:
        start_time = time.Time.now() - time.TimeDelta(1.0*u.day)
    if end_time is None:
        end_time = time.Time.now()

    obstable = get_tap_client().search("""
    SELECT field,ccdid,qid,fid,expid,obsjd,exptime,seeing,airmass,maglimit
    FROM ztf.ztf_current_meta_sci WHERE (obsjd BETWEEN %s AND %s)
    """ % (start_time.jd, end_time.jd)).to_table()
    obstable = obstable.filled()
    obstable['subfield_id'] = (obstable['ccdid']-1)*4 + obstable['qid']

    obs_grouped_by_exp = obstable.group_by('expid').groups
    for expid, rows in zip(obs_grouped_by_exp.keys, obs_grouped_by_exp):
        # We don't use the secondary grid
        if int(rows['field'][0]) > 879:
            continue
        for row in rows:
            expobs = time.Time(row['obsjd'], format='jd').datetime,
            models.db.session.merge(
                models.Observation(telescope='ZTF', field_id=int(row['field']),
                                   observation_id=int(row['expid']),
                                   expobs=expobs,
                                   exposure_time=int(row['exptime']),
                                   filter_id=int(row['fid']),
                                   airmass=float(row['airmass']),
                                   seeing=float(row['seeing']),
                                   limmag=float(row['maglimit']),
                                   subfield_id=int(row['subfield_id']),
                                   successful=1))
        subfield_ids = rows['subfield_id'].tolist()
        quadrantIDs = np.arange(1, 65)
        missing_quadrants = np.setdiff1d(quadrantIDs, subfield_ids)
        for missing_quadrant in missing_quadrants:
            expobs = time.Time(rows['obsjd'][0], format='jd').datetime,
            models.db.session.merge(
                models.Observation(telescope='ZTF',
                                   field_id=int(rows['field'][0]),
                                   observation_id=int(rows['expid'][0]),
                                   expobs=expobs,
                                   exposure_time=int(rows['exptime'][0]),
                                   filter_id=int(rows['fid'][0]),
                                   airmass=float(rows['airmass'][0]),
                                   subfield_id=int(missing_quadrant),
                                   successful=0))
    models.db.session.commit()
