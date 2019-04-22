import numpy as np
from pyvo.dal import TAPService
from celery.task import PeriodicTask
from celery.utils.log import get_task_logger

from . import celery
from .. import models

log = get_task_logger(__name__)

__all__ = ('ztf_references',)


def get_tap_client():
    url = 'https://irsa.ipac.caltech.edu/TAP'
    return TAPService(url)


@celery.task(base=PeriodicTask, shared=False, run_every=3600)
def ztf_references():

    refstable = get_tap_client().search("""
    SELECT field, ccdid, qid, fid, maglimit FROM ztf.ztf_current_meta_ref
    WHERE (nframes >= 15) AND (startobsdate >= '2018-02-05T00:00:00Z')
    AND (field < 880)
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
