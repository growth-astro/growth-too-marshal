from datetime import datetime, timedelta

from celery.task import PeriodicTask
from celery.utils.log import get_task_logger
import requests

from . import celery
from .. import models

log = get_task_logger(__name__)

BASE_URL = 'http://skipper.caltech.edu:8080/cgi-bin/growth/'
PROGRAM_NAMES = [
    # 'DECAM GW Followup',  # Duplicate record issues
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


@celery.task(ignore_result=True, shared=False)
def update_candidate_details(name, growth_marshal_id):
    """Fetch a specific source's autoannotations from the GROWTH marshal and
    create a string with the autoannotations available."""
    json = _get_json('source_summary.cgi', sourceid=growth_marshal_id)

    annotations = '; '.join(
        f"{auto['username']}, {auto['datatype']}, {auto['comment']}"
        for auto in json['autoannotations'])

    photometry = [
        models.CandidatePhotometry(
            dateobs=datetime.fromisoformat(s['obsdate']),
            fil=s['filter'],
            instrument=s['instrument'],
            limmag=s['limmag'],
            mag=s['magpsf'],
            magerr=s['sigmamagpsf'],
            exptime=s['exptime'],
            programid=s['programid']
        ) for s in json['uploaded_photometry']]

    models.db.session.merge(models.Candidate(
        name=name, photometry=photometry, autoannotations=annotations))
    models.db.session.commit()


@celery.task(base=PeriodicTask, shared=False, run_every=1200)
def update_candidates():
    """Fetch the candidates present in the GROWTH marshal
    for the MMA science programs and store them in the local db."""
    for s in get_candidates(get_program_ids()):
        # Find old row, if any
        old = models.Candidate.query.get(s['name'])

        # Create or update row
        last_updated = datetime.fromisoformat(s['last_updated'])
        name = s['name']
        growth_marshal_id = s['id']
        models.db.session.merge(models.Candidate(
            name=name,
            growth_marshal_id=growth_marshal_id,
            subfield_id=s.get('rcid'),
            creationdate=datetime.fromisoformat(s['creationdate']),
            classification=s['classification'],
            redshift=s.get('redshift'),
            iauname=s['iauname'],
            field_id=s.get('field'),
            candid=s.get('candid'),
            ra=s['ra'],
            dec=s['dec'],
            last_updated=last_updated))
        models.db.session.commit()

        # If this is a new row, or if it has been recently updated in
        # the GROWTH marshal, then fetch new photometry and annotations
        dt = timedelta(seconds=60)
        if old is None or last_updated - old.last_updated > dt:
            update_candidate_details.delay(name, growth_marshal_id)
