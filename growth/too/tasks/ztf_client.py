import os
from astropy import time
import astropy.units as u
from astropy.table import Table
from celery.task import PeriodicTask
from celery.utils.log import get_task_logger
from celery.local import PromiseProxy
import numpy as np
import pyvo.dal
import requests

from . import celery
from .. import models

log = get_task_logger(__name__)

__all__ = ('ztf_references', 'ztf_obs', 'ztf_depot')

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


@celery.task(base=PeriodicTask, shared=False, run_every=3600)
def ztf_depot(start_time=None, end_time=None):
    """ZTF depot reader.

    Ingests information about images from all program ids
    (including program_id = 1) based on the nightly summary.
    This supplements what is available from the TAP interface,
    where information about public images is not available.

    Parameters
    ----------
    start_time : astropy.Time
        Start time of request.
    end_time : astropy.Time)
        End time of request.

    """

    if start_time is None:
        start_time = time.Time.now() - time.TimeDelta(1.0*u.day)
    if end_time is None:
        end_time = time.Time.now()

    depotdir = 'https://ztfweb.ipac.caltech.edu/ztf/depot'

    mjds = np.arange(np.floor(start_time.mjd), np.ceil(end_time.mjd))
    for mjd in mjds:
        this_time = time.Time(mjd, format='mjd')
        dstr = this_time.iso.split(" ")[0].replace("-", "")

        url = os.path.join(depotdir, '%s/goodsubs_%s.txt' % (dstr, dstr))
        deptable = get_ztf_depot_table(url)
        if len(deptable) == 0:
            continue

        obs_grouped_by_jd = deptable.group_by('jd').groups
        for jd, rows in zip(obs_grouped_by_jd.keys, obs_grouped_by_jd):
            obstime = time.Time(rows['jd'][0], format='jd').datetime
            for row in rows:
                models.db.session.merge(
                    models.Observation(telescope='ZTF',
                                       field_id=int(row['field']),
                                       observation_id=int(row['expid']),
                                       obstime=obstime,
                                       limmag=row['scimaglim'],
                                       exposure_time=int(30),  # fixme
                                       filter_id=int(row['fid']),
                                       subfield_id=int(row['rcid']),
                                       successful=1))
            subfield_ids = set(rows['rcid'])
            quadrant_ids = set(range(64))
            missing_quadrants = quadrant_ids - subfield_ids
            for missing_quadrant in missing_quadrants:
                models.db.session.merge(
                    models.Observation(telescope='ZTF',
                                       field_id=int(rows['field'][0]),
                                       observation_id=int(row['expid']),
                                       obstime=obstime,
                                       exposure_time=int(30),  # fixme
                                       filter_id=int(row['fid']),
                                       subfield_id=int(missing_quadrant),
                                       successful=0))
    models.db.session.commit()


def get_ztf_depot_table(url):
    with requests.get(url) as r:
        deptable = Table.read(r.text, format='ascii.fixed_width',
                              data_start=2, data_end=-1)
    return deptable
