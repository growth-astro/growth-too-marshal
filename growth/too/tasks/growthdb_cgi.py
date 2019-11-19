from datetime import datetime

from celery.task import PeriodicTask
from celery.utils.log import get_task_logger
import requests

from . import celery
from .. import models

log = get_task_logger(__name__)

BASE_URL = 'http://skipper.caltech.edu:8080/cgi-bin/growth/'
PROGRAM_NAMES = [
    'DECAM GW Followup',
    'Afterglows of Fermi Gamma Ray Bursts',
    'Electromagnetic Counterparts to Neutrinos',
    'Electromagnetic Counterparts to Gravitational Waves']


def _get_json(path, **kwargs):
    response = requests.get(BASE_URL + path, params=kwargs)
    response.raise_for_status()
    return response.json()


def get_program_ids():
    """Get program IDs from the GROWTH marshal."""
    json = _get_json('list_programs.cgi')
    programs = {p['name']: p['programidx'] for p in json}
    for key in PROGRAM_NAMES:
        try:
            yield programs[key]
        except KeyError:
            log.error("The user does not have access to the GROWTH Marshal "
                      "program '%s'", key)


def get_candidates(program_ids):
    """Get sources for a list of program IDs from the GROWTH marshal."""
    result = {}
    for program_id in program_ids:
        json = _get_json('list_program_sources.cgi', programidx=program_id)
        result.update({s['name']: s for s in json})
    return list(result.values())


def get_source_autoannotations(sourceid):
    """Fetch a specific source's autoannotations from the GROWTH marshal and
    create a string with the autoannotations available."""
    json = _get_json('source_summary.cgi', sourceid=sourceid)
    autoannotations = json['autoannotations']
    autoannotations_string = '; '.join(
        f"{auto['username']}, {auto['datatype']}, {auto['comment']}"
        for auto in autoannotations)
    return autoannotations_string


@celery.task(base=PeriodicTask, shared=False, run_every=1200)
def update_candidates():
    """Fetch the candidates present in the GROWTH marshal
    for the MMA science programs and store them in the local db."""
    for s in get_candidates(get_program_ids()):
        models.db.session.merge(
            models.Candidate(
                name=s['name'],
                subfield_id=s.get('rcid'),
                creationdate=datetime.fromisoformat(s['creationdate']),
                classification=s['classification'],
                redshift=s.get('redshift'),
                iauname=s['iauname'],
                field_id=s.get('field'),
                candid=s.get('candid'),
                ra=s['ra'],
                dec=s['dec'],
                lastmodified=datetime.fromisoformat(s['lastmodified']),
                autoannotations=get_source_autoannotations(s['id'])))
    models.db.session.commit()
