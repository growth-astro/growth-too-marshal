import json
import os.path
import requests
import subprocess
import tempfile
import urllib.parse
import numpy as np
from astropy.table import Table
from astropy.coordinates import SkyCoord, EarthLocation, get_moon
from astropy.time import Time
import astropy.units as u
from astroplan import Observer, is_always_observable
from astroplan.constraints import AltitudeConstraint
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
        tab = get_growthindia_table(json_data)
        tab.write(f, format='csv')
        f.seek(0)
        dest = os.path.join(GROWTH_INDIA_PATH,
                            json_data["queue_name"] + '.csv')
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
    decam_dict["propid"] = "2019B-0353",
    decam_dict["dec"] = data_row["dec"]+dec_diff,
    decam_dict["exptime"] = data_row["exposure_time"],
    decam_dict["wait"] = "False"

    return decam_dict


def get_growthindia_table(json_data, sunrise_hor=-12, horizon=20,
                          priority=10000, domesleep=100):
    """Make .csv file in GIT toO format for a given .json file"""
    t = Table(rows=json_data['targets'])
    coords = SkyCoord(ra=t['ra'], dec=t['dec'], unit=(u.degree, u.degree))
    hanle = EarthLocation(lat=32.77889*u.degree,
                          lon=78.96472*u.degree,
                          height=4500*u.m)
    iao = Observer(location=hanle, name="GIT", timezone="Asia/Kolkata")

    twilight_prime = iao.sun_rise_time(Time.now(), which="next",
                                       horizon=sunrise_hor*u.deg) - 12*u.hour
    targets_rise_time = iao.target_rise_time(twilight_prime,
                                             coords,
                                             which="nearest",
                                             horizon=horizon*u.degree)
    targets_set_time = iao.target_set_time(targets_rise_time, coords,
                                           which="next",
                                           horizon=horizon*u.degree)
    rise_time_IST = np.array([(targets_rise_time + 5.5*u.hour).isot])
    set_time_IST = np.array([(targets_set_time + 5.5*u.hour).isot])
    tend = targets_set_time
    mooncoords = get_moon(tend, hanle)
    sep = mooncoords.separation(coords)
    dic = {}
    dic['x'] = ['']*len(t)
    dic['u'] = ['']*len(t)
    dic['g'] = ['']*len(t)
    dic['r'] = ['']*len(t)
    dic['i'] = ['']*len(t)
    dic['z'] = ['']*len(t)
    target = ['EMGW']*len(t)
    bands = {1: 'g', 2: 'r', 3: 'i', 4: 'z', 5: 'u'}

    for i in range(len(t)):
        filt = bands[t[i]['filter_id']]
        dic[filt][i] = '1X%i' % (t[i]['exposure_time'])

    del t['request_id', 'filter_id', 'program_pi', 'program_id',
          'subprogram_name', 'exposure_time']
    t['field_id'].name = 'tile_id'
    t['dec'].name = 'Dec'
    domesleeparr = np.zeros(len(t)) + domesleep
    priority = np.zeros(len(t)) + priority
    minalt = AltitudeConstraint(min=horizon*u.degree)
    always_up = is_always_observable(minalt, iao, coords, Time(twilight_prime,
                                     twilight_prime+12*u.hour))
    rise_time_IST[np.where(always_up)] = (twilight_prime +
                                          5.5*u.hour).isot
    set_time_IST[np.where(always_up)] = (twilight_prime +
                                         24*u.hour +
                                         5.5*u.hour).isot
    ras_format = []
    decs_format = []
    ras_format = coords.ra.to_string(u.hour, sep=':')
    decs_format = coords.dec.to_string(u.degree, sep=':')
    # Add columns
    t['domesleep'] = domesleeparr
    t['Priority'] = priority
    t['dec'] = decs_format
    t['rise_time_IST'] = rise_time_IST
    t['set_time_IST'] = set_time_IST
    t['moon_angle'] = sep
    t['RA'] = ras_format
    t['Target'] = target
    t['x'] = dic['x']
    t['u'] = dic['u']
    t['g'] = dic['g']
    t['r'] = dic['r']
    t['i'] = dic['i']
    t['z'] = dic['z']

    return t
