import requests
import json
import numpy as np
import astropy.units as u
import datetime
from celery.task import PeriodicTask
from celery.utils.log import get_task_logger
from celery.local import PromiseProxy
import celery

from .. import models

"""
Reminder for the relevant program names:
decam_programidx=program_dict['DECAM GW Followup']
em_gw_programidx=program_dict['Electromagnetic Counterparts to Gravitational Waves']
fermi_programidx=program_dict['Afterglows of Fermi Gamma Ray Bursts']
neutrino_programidx=program_dict['Electromagnetic Counterparts to Neutrinos']
"""


def get_programidx(program_name):
    """ Given a program name, it returns the programidx """

    r = requests.post('http://skipper.caltech.edu:8080/cgi-bin/growth/list_programs.cgi')
    programs=json.loads(r.text)
    program_dict={p['name']:p['programidx'] for i,p in enumerate(programs)}

    try:
        return program_dict[program_name]
    except KeyError:
        print(f'The user {username} does not have access to the program {program_name}')
        return None


def get_source_autoannotations(sourceid):
    """ Fetch a specific source's autoannotations from the GROWTH marshal and 
    create a string with the autoannotations available. """
    r = requests.post('http://skipper.caltech.edu:8080/cgi-bin/growth/source_summary.cgi', data={'sourceid':str(sourceid)
}) 
    r.raise_for_status()
    summary=json.loads(r.text)
    autoannotations=summary['autoannotations']
    autoannotations_string = ""
    for auto in autoannotations:
        autoannotations_string = f"{autoannotations_string}; \n {auto['username']}, {auto['type']}, {auto['comment']}" 

    return autoannotations_string


def get_candidates_growth_marshal(program_name):
    """ Query the GROWTH db for the science programs """

    programidx=get_programidx(program_name)
    if programidx==None:
        return None
    r = requests.post('http://skipper.caltech.edu:8080/cgi-bin/growth/list_program_sources.cgi', data={'programidx':str(programidx)})
    r.raise_for_status()
    sources=json.loads(r.text)
    """ Add autoannotations """
    for s, i in zip(sources, np.arange(len(sources))):
        autoannotations=get_source_autoannotations(s["id"])
        sources[i]['autoannotations'] = autoannotations

    return sources


def update_local_db_growthmarshal(sources, program_name):
    """ Takes the candidates fetched from the GROWTH marshal and 
    updates the local database using SQLAlchemy. """
    for s in sources:
        print("printing s:", s)
        creationdate_list_int=[int(d) for d in s['creationdate'].split('-')]
        lastmodified_list_int=[int(d) for d in s['lastmodified'].split('-')]
        #autoannotations_string = get_source_autoannotations(sourceid, username, password)

        try:
            rcid = int(s['rcid'])
            field = int(s['field'])
            candid = int(s['candid'])
        except:
            rcid = None
            field = None
            candid = None

        try:
            redshift = float(s['redshift'])
        except:
            redshift = None

        models.db.session.merge( 
            models.Candidates(
                name = s['name'],
                subfield_id = rcid,
                creationdate = datetime.datetime(creationdate_list_int[0],\
                creationdate_list_int[1],creationdate_list_int[2]),
                classification = s['classification'],
                redshift = redshift,
                iauname = s['iauname'],
                field_id = field,
                candid = candid,
                ra = float(s['ra']),
                dec = float(s['dec']),
                lastmodified = datetime.datetime(lastmodified_list_int[0],\
                lastmodified_list_int[1],lastmodified_list_int[2]),
                autoannotations = s['autoannotations']
                )
        )
    models.db.session.commit()


@celery.task(base=PeriodicTask, shared=False, run_every=datetime.timedelta(seconds=180))
def fetch_candidates_growthmarshal():
    """Fetch the candidates present in the GROWTH marshal
    for the MMA science programs and store them in the local db. """ 

    program_names = ['DECAM GW Followup', 'Afterglows of Fermi Gamma Ray Bursts', \
    'Electromagnetic Counterparts to Neutrinos', 'Electromagnetic Counterparts to Gravitational Waves']

    for program_name in program_names:
        sources=get_candidates_growth_marshal(program_name)
        update_local_db_growthmarshal(sources, program_name)

    return
