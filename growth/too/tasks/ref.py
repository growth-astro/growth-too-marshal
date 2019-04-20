import sys
import subprocess
import shlex
from subprocess import PIPE
import numpy as np
import tempfile
import requests

from celery.task import PeriodicTask
from celery.utils.log import get_task_logger

from ..flask import app
from . import celery
from .. import models

log = get_task_logger(__name__)

__all__ = ('ztf_references',)


@app.task(base=PeriodicTask, shared=False, run_every=3600)
def ztf_references():

    user = app.config['IRSA_USERNAME']
    pw = app.config['IRSA_PASSWORD']

    if not 'IRSA_USERNAME' in app.config or not 'IRSA_PASSWORD' in app.config:
        log.info('Missing IRSA_USERNAME or IRSA_PASSWORD...')
        return

    params = (('josso_cmd', 'login'),
              ('josso_username', app.config['IRSA_USERNAME']),
              ('josso_password', app.config['IRSA_PASSWORD']))

    session = requests.Session()
    session.get('https://irsa.ipac.caltech.edu/account/signon/login.do', params=params, verify=False)
    params = (('WHERE', 'nframes>=15 AND startobsdate>=\'2018-02-05T00:00:00Z\''),)
    r = session.get('https://irsa.ipac.caltech.edu/ibe/search/ztf/products/ref', params=params, verify=False)

    lines = r.text.split('\n')
    refs = {}
    mags = {}
    for line in lines:
        lineSplit = line.split(" ")
        lineSplit = list(filter(None,lineSplit))
    
        if not len(lineSplit) == 35: continue
    
        fieldID, filt, maglimcat = int(lineSplit[1]), int(lineSplit[5]), float(lineSplit[9])
        if not fieldID in refs:
            refs[fieldID] = []
            mags[fieldID] = []
        refs[fieldID].append(filt)
        mags[fieldID].append(maglimcat)
    for fieldID in refs.keys():
        refs[fieldID], idx = np.unique(refs[fieldID],return_index=True)
        mags[fieldID] = np.array(mags[fieldID])[idx]
        
    if len(refs.keys()) == 0:
        log.info('References missing... quitting.')
        return

    fields = models.Field.query.filter_by(telescope='ZTF').all()
    for field in fields:
        fieldID = field.field_id
        if fieldID in refs:
            ref_filter_ids = refs[fieldID].tolist()
            ref_filter_mags = mags[fieldID].tolist()

            field.reference_filter_ids = ref_filter_ids
            models.db.session.merge(field)
