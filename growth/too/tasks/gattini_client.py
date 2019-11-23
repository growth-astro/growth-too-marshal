from astropy import time
import astropy.units as u
from celery.task import PeriodicTask
from celery.utils.log import get_task_logger

from ..flask import app
from . import celery
from .. import models

import psycopg2
import psycopg2.extras

log = get_task_logger(__name__)

__all__ = ('gattini_obs')


def get_obs(start_time, end_time):

    conn = psycopg2.connect(host=app.config["GATTINI_HOST"],
                            dbname=app.config["GATTINI_DBNAME"],
                            user=app.config["GATTINI_USER"],
                            password=app.config["GATTINI_PASSWORD"])
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = 'SELECT ss.field, ss.fieldseq, min(ss.datebeg) AS utstart, ' +\
            'avg(sp.limmagpsf) AS limmag FROM splitstacks ss INNER JOIN ' +\
            'squadphoto sp ON sp.stackquadid = ss.stackquadid WHERE jd ' +\
            '> %.5f AND jd < %.5f ' % (start_time.jd, end_time.jd) +\
            'GROUP BY field, fieldseq ORDER BY utstart;'

    cur.execute(query)
    result = cur.fetchall()

    cur.close()
    conn.close()

    return result


@celery.task(base=PeriodicTask, shared=False, run_every=3600)
def gattini_obs(start_time=None, end_time=None):

    if start_time is None:
        start_time = time.Time.now() - time.TimeDelta(1.0*u.day)
    if end_time is None:
        end_time = time.Time.now()

    obstable = get_obs(start_time, end_time)

    for ii, row in enumerate(obstable):
        field_id, obsid, obstime, limmag = row
        models.db.session.merge(
            models.Observation(telescope='Gattini',
                               field_id=int(field_id),
                               observation_id=int(obsid),
                               obstime=obstime,
                               exposure_time=65,
                               filter_id=5,
                               limmag=limmag,
                               successful=1))
    models.db.session.commit()
