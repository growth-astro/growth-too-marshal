from astropy import table
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
    field, ccdid, qid, fid, maglimit FROM ztf.ztf_current_meta_ref
    WHERE nframes >= 15 AND startobsdate >= '2018-02-05T00:00:00Z'
    """).to_table()

    refs = table.unique(refstable, keys=['field', 'fid'])

    reference_images = \
        {group[0]['field']: group['fid'].astype(int).tolist()
         for group in refs.group_by('field').groups}
    reference_mags = \
        {group[0]['field']: group['maglimit'].tolist()
         for group in refs.group_by('field').groups}

    fields = models.Field.query.filter_by(telescope='ZTF').all()
    for field in fields:
        field_id = field.field_id

        ref_filter_ids = reference_images.get(field_id, [])
        ref_filter_mags = []
        for val in reference_mags.get(field_id, []):
            ref_filter_mags.append(val)

        field.reference_filter_ids = ref_filter_ids
        field.reference_filter_mags = ref_filter_mags
        models.db.session.merge(field)

    models.db.session.commit()
