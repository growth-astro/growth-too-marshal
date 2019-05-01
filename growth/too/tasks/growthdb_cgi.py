import requests
import json
import pdb
import astropy.units as u
import datetime
from celery.task import PeriodicTask
from celery.utils.log import get_task_logger
from celery.local import PromiseProxy

from tasks import celery
import models

'''
Reminder of relevant program names:

decam_programidx=program_dict['DECAM GW Followup']
em_gw_programidx=program_dict['Electromagnetic Counterparts to Gravitational Waves']
fermi_programidx=program_dict['Afterglows of Fermi Gamma Ray Bursts']
neutrino_programidx=program_dict['Electromagnetic Counterparts to Neutrinos']
'''


def get_programidx(program_name, username, password):
    ''' Given a program name, it returns the programidx '''

    r = requests.post('http://skipper.caltech.edu:8080/cgi-bin/growth/list_programs.cgi', auth=(username, password))
    programs=json.loads(r.text)
    program_dict={p['name']:p['programidx'] for i,p in enumerate(programs)}

    try:
        return program_dict[program_name]
    except KeyError:
        print(f'The user {username} does not have access to the program {program_name}')
        return None


def get_candidates_growth_marshal(program_name, username, password):
    ''' Query the GROWTH db for the science programs '''

    programidx=get_programidx(program_name, username, password)
    if programidx==None:
        return None
    r = requests.post('http://skipper.caltech.edu:8080/cgi-bin/growth/list_program_sources.cgi', auth=(username, password), data={'programidx':str(programidx)})
    sources=json.loads(r.text)
    '''Note: the cgi returns a list of sources with unique ZTF names '''
    set_sources={s['name'] for s in sources}
    pdb.set_trace()
    return sources

  
def update_local_db_growthmarshal(sources, program_name):
    ''' Takes the candidates fetched from the GROWTH marshal and 
    updates the local database using SQLAlchemy. '''
    for s in sources:
        creationdate_list_int=[int(d) for d in s['creationdate'].split('-')]
        lastmodified_list_int=[int(d) for d in s['lastmodified'].split('-')]
        models.db.session.merge( 
            models.Candidates(
                name = s['name'],
                rcid = int(s['rcid']),
                creationdate = datetime.datetime(creationdate_list_int[0],\
                creationdate_list_int[1],creationdate_list_int[2]),
                classification = s['classification'],
                redshift = float(s['redshift']),
                iauname =s['iauname'],
                field = int(s['field']),
                candid = int(s['candid']),
                ra = float(s['ra']),
                dec = float(s['dec']),
                lastmodified = datetime.datetime(lastmodified_list_int[0],\
                lastmodified_list_int[1],lastmodified_list_int[2])
                ) 
        )

    models.db.session.commit()
    return


@celery.task(base=PeriodicTask, shared=False, run_every=180)
def fetch_candidates_growthmarshal():
    program_names=['DECAM GW Followup', 'Afterglows of Fermi Gamma Ray Bursts', \
    'Electromagnetic Counterparts to Neutrinos', 'Electromagnetic Counterparts to Gravitational Waves']
    ''' EM Counterparts to GWs, being last in the list, overrides '''
    for program_name in program_names:
        sources=get_candidates_growth_marshal(program_name, username, password)
        update_local_db_growthmarshal(sources, program_name)
    return
