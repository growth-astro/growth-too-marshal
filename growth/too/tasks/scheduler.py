import json
import os.path
import requests
import subprocess
import tempfile
import urllib.parse

from . import celery
from .. import models
from .. import views

ZTF_URL = 'http://tunnel:9999'
"""URL for the P48 scheduler."""


GATTINI_PATH = 'mcoughlin@schoty.caltech.edu:gattini/too'
"""``scp`` path for Gattini schedules."""


KPED_PATH = 'mcoughlin@schoty.caltech.edu:kped/too'
"""``scp`` path for KPED schedules."""


GROWTH_INDIA_PATH = 'mcoughlin@schoty.caltech.edu:git/too'
"""``scp`` path for GROWTH-India schedules."""


DECAM_PATH = 'mcoughlin@schoty.caltech.edu:decam/too'
"""``scp`` path for DECam schedules."""


@celery.task(shared=False)
def ping(telescope):
    if telescope == "ZTF":
        ping_ztf()
    elif telescope == "Gattini":
        ping_gattini()
    elif telescope == "DECam":
        ping_decam()
    elif telescope == "KPED":
        ping_kped()
    elif telescope == "GROWTH-India":
        ping_growth_india()
    else:
        raise ValueError('Should not be reached')


@celery.task(shared=False)
def submit(dateobs, telescope, plan_name):

    plan = models.Plan.query.filter_by(dateobs=dateobs, telescope=telescope,
                                       plan_name=plan_name).one()
    json_data, queue_name = views.get_json_data(plan)

    if telescope == "ZTF":
        schedule_ztf(json_data)
    elif telescope == "Gattini":
        schedule_gattini(json_data)
    elif telescope == "DECam":
        schedule_decam(json_data, queue_name)
    elif telescope == "KPED":
        schedule_kped(json_data)
    elif telescope == "GROWTH-India":
        schedule_growth_india(json_data)
    else:
        raise ValueError('Should not be reached')


@celery.task(shared=False)
def submit_manual(telescope, json_data, queue_name):

    if telescope == "ZTF":
        schedule_ztf(json_data)
    elif telescope == "Gattini":
        schedule_gattini(json_data)
    elif telescope == "DECam":
        schedule_decam(json_data, queue_name)
    elif telescope == "KPED":
        schedule_kped(json_data)
    elif telescope == "GROWTH-India":
        schedule_growth_india(json_data)
    else:
        raise ValueError('Should not be reached')


@celery.task(shared=False)
def ping_ztf():
    """Check connectivity with ZTF scheduler."""
    url = urllib.parse.urljoin(ZTF_URL, 'queues')
    requests.get(url, json={}).raise_for_status()
    return True


@celery.task(shared=False)
def ping_gattini():
    """Check connectivity with Gattini scheduler."""
    with tempfile.NamedTemporaryFile(mode='w') as f:
        print('This is a test file to check connectivity with the ToO '
              'Marshal. It can safely be deleted.', file=f)
        f.flush()

        dest = os.path.join(GATTINI_PATH, '.ztf-test')
        subprocess.run(['scp', '-oBatchMode=yes', '-v', f.name, dest],
                       check=True)


@celery.task(shared=False)
def ping_kped():
    """Check connectivity with KPED scheduler."""
    with tempfile.NamedTemporaryFile(mode='w') as f:
        print('This is a test file to check connectivity with the ToO '
              'Marshal. It can safely be deleted.', file=f)
        f.flush()

        dest = os.path.join(KPED_PATH, '.ztf-test')
        subprocess.run(['scp', '-oBatchMode=yes', '-v', f.name, dest],
                       check=True)


@celery.task(shared=False)
def ping_growth_india():
    """Check connectivity with GROWTH-India scheduler."""
    with tempfile.NamedTemporaryFile(mode='w') as f:
        print('This is a test file to check connectivity with the ToO '
              'Marshal. It can safely be deleted.', file=f)
        f.flush()

        dest = os.path.join(GROWTH_INDIA_PATH, '.ztf-test')
        subprocess.run(['scp', '-oBatchMode=yes', '-v', f.name, dest],
                       check=True)


@celery.task(shared=False)
def ping_decam():
    """Check connectivity with DECam scheduler."""
    with tempfile.NamedTemporaryFile(mode='w') as f:
        print('This is a test file to check connectivity with the ToO '
              'Marshal. It can safely be deleted.', file=f)
        f.flush()

        dest = os.path.join(DECAM_PATH, '.ztf-test')
        subprocess.run(['scp', '-oBatchMode=yes', '-v', f.name, dest],
                       check=True)


@celery.task(ignore_result=True, shared=False)
def schedule_ztf(json_data):
    r = requests.put(urllib.parse.urljoin(ZTF_URL, 'queues'),
                     json={'targets': json_data["targets"],
                           'queue_name': json_data["queue_name"],
                           'validity_window_mjd':
                               json_data["validity_window_mjd"],
                           'queue_type': 'list'})
    r.raise_for_status()


@celery.task(ignore_result=True, shared=False)
def schedule_gattini(json_data):

    with tempfile.NamedTemporaryFile(mode='w') as f:
        json.dump(json_data, f, indent=4, sort_keys=True)
        f.flush()

        dest = os.path.join(GATTINI_PATH, json_data["queue_name"] + '.json')
        subprocess.run(['scp', '-oBatchMode=yes', '-v', f.name, dest],
                       check=True)


@celery.task(ignore_result=True, shared=False)
def schedule_kped(json_data):

    with tempfile.NamedTemporaryFile(mode='w') as f:
        json.dump(json_data, f, indent=4, sort_keys=True)
        f.flush()

        dest = os.path.join(KPED_PATH, json_data["queue_name"] + '.json')
        subprocess.run(['scp', '-oBatchMode=yes', '-v', f.name, dest],
                       check=True)


@celery.task(ignore_result=True, shared=False)
def schedule_growth_india(json_data):

    with tempfile.NamedTemporaryFile(mode='w') as f:
        json.dump(json_data, f, indent=4, sort_keys=True)
        f.flush()

        dest = os.path.join(GROWTH_INDIA_PATH,
                            json_data["queue_name"] + '.json')
        subprocess.run(['scp', '-oBatchMode=yes', '-v', f.name, dest],
                       check=True)


@celery.task(ignore_result=True, shared=False)
def schedule_decam(json_data, queue_name):

    with tempfile.NamedTemporaryFile(mode='w') as f:
        json.dump(json_data, f, indent=4, sort_keys=True)
        f.flush()

        dest = os.path.join(DECAM_PATH, queue_name + '.json')
        subprocess.run(['scp', '-oBatchMode=yes', '-v', f.name, dest],
                       check=True)


def get_decam_dict(data_row, queue_name, cnt, nrows,
                   ra_diff=0.0, dec_diff=0.0):

    bands = {1: 'g', 2: 'r', 3: 'i', 4: 'z', 5: 'J'}

    decam_dict = {}
    decam_dict["count"] = 1
    decam_dict["comment"] = "%s: %d of %d" % (queue_name, cnt, nrows)
    decam_dict["seqtot"] = nrows,
    decam_dict["seqnum"] = cnt,
    decam_dict["expType"] = "object",
    decam_dict["object"] = "%s: %d of %d" % (queue_name, cnt, nrows)
    decam_dict["filter"] = bands[data_row["filter_id"]][0],
    decam_dict["program"] = "GROWTH DECam GW",
    decam_dict["RA"] = data_row["ra"]+ra_diff,
    decam_dict["seqid"] = queue_name,
    decam_dict["propid"] = "2019A-0205",
    decam_dict["dec"] = data_row["dec"]+dec_diff,
    decam_dict["exptime"] = data_row["exposure_time"],
    decam_dict["wait"] = "False"

    return decam_dict
