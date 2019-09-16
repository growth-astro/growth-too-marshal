import requests
import datetime
import logging

from astropy.time import Time
from celery.task import PeriodicTask
import celery

from .. import models

"""
Reminder for the relevant program names:
decam_programidx=program_dict['DECAM GW Followup']
em_gw_programidx=program_dict['Electromagnetic Counterparts
to Gravitational Waves']
fermi_programidx=program_dict['Afterglows of Fermi Gamma Ray Bursts']
neutrino_programidx=program_dict['Electromagnetic Counterparts
to Neutrinos']
"""


def get_programidx(program_name):
    """Given a program name, it returns the programidx"""

    r = requests.post(
        'http://skipper.caltech.edu:8080/cgi-bin/growth/list_programs.cgi')
    r.raise_for_status()
    programs = r.json()
    program_dict = {p['name']: p['programidx'] for p in enumerate(programs)}

    try:
        return program_dict[program_name]
    except KeyError:
        logging.error(f"The user does not have access to \
            the GROWTH Marshal program '{program_name}'")


def get_source_autoannotations(sourceid):
    """Fetch a specific source's autoannotations from the GROWTH marshal and
    create a string with the autoannotations available."""
    r = requests.post(
        'http://skipper.caltech.edu:8080/cgi-bin/growth/source_summary.cgi',
        data={'sourceid': str(sourceid)})
    r.raise_for_status()
    summary = r.json()
    autoannotations = summary['autoannotations']
    autoannotations_string = '; '.join(
        f"{auto['username']}, {auto['type']}, {auto['comment']}"
        for auto in autoannotations)

    return autoannotations_string


def get_candidates_growth_marshal(program_name):
    """Query the GROWTH db for the science programs"""

    programidx = get_programidx(program_name)
    if programidx is None:
        return
    r = requests.post(
        'http://skipper.caltech.edu:8080/cgi-bin/growth/list\
        _program_sources.cgi',
        data={'programidx': str(programidx)})
    r.raise_for_status()
    sources = r.json()
    # Add autoannotations
    for source in sources:
        yield dict(
            source,
            annotations=get_source_autoannotations(source["id"]))


def update_local_db_growthmarshal(sources, program_name):
    """Takes the candidates fetched from the GROWTH marshal and
    updates the local database using SQLAlchemy."""

    for s in sources:
        creationdate = Time(s['creationdate'], fromat='iso').datetime
        lastmodified = Time(s['lastmodified'], fromat='iso').datetime

        try:
            rcid = int(s['rcid'])
        except (KeyError, ValueError):
            rcid = None
        try:
            field = int(s['field'])
        except (KeyError, ValueError):
            field = None
        try:
            candid = int(s['candid'])
        except (KeyError, ValueError):
            candid = None
        try:
            redshift = float(s['redshift'])
        except (KeyError, ValueError):
            redshift = None

        models.db.session.merge(
            models.Candidate(
                name=s['name'],
                subfield_id=rcid,
                creationdate=creationdate,
                classification=s['classification'],
                redshift=redshift,
                iauname=s['iauname'],
                field_id=field,
                candid=candid,
                ra=float(s['ra']),
                dec=float(s['dec']),
                lastmodified=lastmodified,
                autoannotations=s['autoannotations']
                )
        )
    models.db.session.commit()


@celery.task(
    base=PeriodicTask,
    shared=False, run_every=datetime.timedelta(seconds=180))
def fetch_candidates_growthmarshal():
    """Fetch the candidates present in the GROWTH marshal
    for the MMA science programs and store them in the local db."""

    program_names = [
        'DECAM GW Followup',
        'Afterglows of Fermi Gamma Ray Bursts',
        'Electromagnetic Counterparts to Neutrinos',
        'Electromagnetic Counterparts to Gravitational Waves'
        ]

    for program_name in program_names:
        sources = get_candidates_growth_marshal(program_name)
        update_local_db_growthmarshal(sources, program_name)
