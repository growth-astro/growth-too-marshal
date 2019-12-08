from astropy import time
import requests
import urllib

from ..flask import app
from . import celery
from .. import models
from .. import views

BASE = 'http://treasuremap.space/api/v0/'
bands = {1: 'g', 2: 'r', 3: 'i', 4: 'z', 5: 'J'}


@celery.task(shared=False)
def observations(dateobs, telescope, observations):

    TARGET = 'pointings'

    if telescope == "ZTF":
        telescope_id = 47
    elif telescope == "Gattini":
        telescope_id = 44
    elif telescope == "DECam":
        telescope_id = 38
    elif telescope == "KPED":
        telescope_id = 45
    elif telescope == "GROWTH-India":
        telescope_id = 46

    event = models.Event.query.get_or_404(dateobs)

    data = {"graceid": event.graceid,
            "api_token": app.config['TREASUREMAP_API_TOKEN']}

    pointings = []
    observation_ids = []
    for ii, observation in enumerate(observations):
        if observation.observation_id in observation_ids:
            continue

        obstime = time.Time(observation.obstime, format="datetime").isot

        pointing = {}
        pointing["ra"] = observation.field.ra
        pointing["dec"] = observation.field.dec
        pointing["band"] = bands[observation.filter_id][0]
        pointing["instrumentid"] = str(telescope_id)
        pointing["depth"] = observation.limmag
        pointing["depth_unit"] = "ab_mag"
        pointing["status"] = "completed"
        pointing["time"] = obstime.isot
        pointings.append(pointing)
        observation_ids.append(observation.observation_id)

    data["pointings"] = pointings
    requests.post(url=BASE+TARGET, json=data)


@celery.task(shared=False)
def plan(dateobs, telescope, plan_name):

    TARGET = 'pointings'

    plan = models.Plan.query.filter_by(dateobs=dateobs, telescope=telescope,
                                       plan_name=plan_name).one()
    json_data, queue_name = views.get_json_data(plan, decam_style=False)

    if telescope == "ZTF":
        depth = 20.5
        telescope_id = 47
    elif telescope == "Gattini":
        depth = 16.5
        telescope_id = 44
    elif telescope == "DECam":
        depth = 22.0
        telescope_id = 38
    elif telescope == "KPED":
        depth = 21.0
        telescope_id = 45
    elif telescope == "GROWTH-India":
        depth = 20.0
        telescope_id = 46

    tstart = time.Time(json_data['validity_window_mjd'][0], format='mjd')
    event = models.Event.query.get_or_404(dateobs)

    data = {"graceid": event.graceid,
            "api_token": app.config['TREASUREMAP_API_TOKEN']}

    pointings = []
    for ii, data_row in enumerate(json_data['targets']):
        pointing = {}
        pointing["ra"] = data_row["ra"]
        pointing["dec"] = data_row["dec"]
        pointing["band"] = bands[data_row["filter_id"]][0]
        pointing["instrumentid"] = str(telescope_id)
        pointing["depth"] = str(depth)
        pointing["depth_unit"] = "ab_mag"
        pointing["status"] = "planned"
        pointing["time"] = tstart.isot
        pointings.append(pointing)

    data["pointings"] = pointings
    requests.post(url=BASE+TARGET, json=data)


@celery.task(shared=False)
def delete_plans(dateobs, telescope):

    TARGET = 'cancel_all'

    event = models.Event.query.get_or_404(dateobs)

    if telescope == "ZTF":
        telescope_id = 47
    elif telescope == "Gattini":
        telescope_id = 44
    elif telescope == "DECam":
        telescope_id = 38
    elif telescope == "KPED":
        telescope_id = 45
    elif telescope == "GROWTH-India":
        telescope_id = 46

    data = {"graceid": event.graceid,
            "api_token": app.config['TREASUREMAP_API_TOKEN'],
            "instrumentid": telescope_id}

    url = "{}/{}?{}".format(BASE, TARGET, urllib.parse.urlencode(data))
    requests.post(url=url)
