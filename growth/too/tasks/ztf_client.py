from astropy import time
import astropy.units as u
from celery.task import PeriodicTask
from celery.utils.log import get_task_logger
from celery.local import PromiseProxy
import numpy as np
import pyvo.dal

from . import celery
from .. import models

log = get_task_logger(__name__)

__all__ = ('ztf_references', 'ztf_obs')

client = PromiseProxy(
    pyvo.dal.TAPService,
    ('https://irsa.ipac.caltech.edu/TAP',))


@celery.task(base=PeriodicTask, shared=False, run_every=3600)
def ztf_obs(start_time=None, end_time=None):

    if start_time is None:
        start_time = time.Time.now() - time.TimeDelta(1.0*u.day)
    if end_time is None:
        end_time = time.Time.now()

    obstable = client.search("""
    SELECT field,rcid,fid,expid,obsjd,exptime,seeing,airmass,maglimit
    FROM ztf.ztf_current_meta_sci WHERE (obsjd BETWEEN {0} AND {1})
    AND (field < 2000)
    """.format(start_time.jd, end_time.jd)).to_table()

    obstable = obstable.filled()
    if len(obstable) == 0:
        log.info('No observations in time window to ingest.')
        return

    obs_grouped_by_exp = obstable.group_by('expid').groups
    for expid, rows in zip(obs_grouped_by_exp.keys, obs_grouped_by_exp):
        for row in rows:
            obstime = time.Time(row['obsjd'], format='jd').datetime,
            models.db.session.merge(
                models.Observation(telescope='ZTF', field_id=int(row['field']),
                                   observation_id=int(row['expid']),
                                   obstime=obstime,
                                   exposure_time=int(row['exptime']),
                                   filter_id=int(row['fid']),
                                   airmass=float(row['airmass']),
                                   seeing=float(row['seeing']),
                                   limmag=float(row['maglimit']),
                                   subfield_id=int(row['rcid']),
                                   successful=1))
        subfield_ids = rows['rcid'].tolist()
        quadrantIDs = np.arange(64)
        missing_quadrants = np.setdiff1d(quadrantIDs, subfield_ids)
        for missing_quadrant in missing_quadrants:
            obstime = time.Time(rows['obsjd'][0], format='jd').datetime,
            models.db.session.merge(
                models.Observation(telescope='ZTF',
                                   field_id=int(rows['field'][0]),
                                   observation_id=int(rows['expid'][0]),
                                   obstime=obstime,
                                   exposure_time=int(rows['exptime'][0]),
                                   filter_id=int(rows['fid'][0]),
                                   airmass=float(rows['airmass'][0]),
                                   subfield_id=int(missing_quadrant),
                                   successful=0))
    models.db.session.commit()


@celery.task(base=PeriodicTask, shared=False, run_every=3600)
def ztf_references():
    refstable = client.search("""
    SELECT field, fid, maglimit FROM ztf.ztf_current_meta_ref
    WHERE (nframes >= 15) AND (startobsdate >= '2018-02-05T00:00:00Z')
    AND (field < 2000)
    """).to_table()

    refs = refstable.group_by(['field', 'fid']).groups.aggregate(np.mean)
    refs = refs.filled()

    refs_grouped_by_field = refs.group_by('field').groups
    for field_id, rows in zip(refs_grouped_by_field.keys,
                              refs_grouped_by_field):
        models.db.session.merge(
            models.Field(telescope='ZTF', field_id=int(field_id[0]),
                         reference_filter_ids=rows['fid'].tolist(),
                         reference_filter_mags=rows['maglimit'].tolist()))
    models.db.session.commit()
