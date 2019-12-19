import requests
import logging

import numpy as np
import celery
from astropy.time import Time
import healpy as hp

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


def select_sources_in_contour(sources_growth_marshal, skymap, level=90):
    """Select only those sources within a given contour
    level of the skymap"""

    skymap_prob = skymap.flat_2d
    sort_idx = np.argsort(skymap_prob)[::-1]
    csm = np.empty(len(skymap_prob))
    csm[sort_idx] = np.cumsum(skymap_prob[sort_idx])
    ipix_keep = np.where(csm <= level/100.)[0]
    nside = hp.pixelfunc.get_nside(skymap_prob)
    sources_growth_marshal_contour = list(s for s in sources_growth_marshal
                                          if (hp.ang2pix(
                                                         nside,
                                                         0.5 * np.pi -
                                                         np.deg2rad(s["dec"]),
                                                         np.deg2rad(s["ra"])
                                                        ) in ipix_keep
                                              )
                                          )

    return sources_growth_marshal_contour


def get_programidx(program_name):
    """Given a program name, it returns the programidx"""

    r = requests.post(
        'http://skipper.caltech.edu:8080/cgi-bin/growth/list_programs.cgi')
    r.raise_for_status()
    programs = r.json()
    program_dict = {p['name']: p['programidx'] for p in programs}

    try:
        return program_dict[program_name]
    except KeyError:
        logging.error(f"The user does not have access to \
            the GROWTH Marshal program '{program_name}'")


def get_source_autoannotations_and_photometry(sourceid):
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
    autoannotations_dict = {
                            f"{auto['type']}": auto['comment']
                            for auto in autoannotations
                           }
    photometry_marshal = list(phot for phot in summary['uploaded_photometry'])

    return autoannotations_string, autoannotations_dict, photometry_marshal


def get_candidates_growth_marshal(program_name, new=False, dateobs=None,
                                  skymap=None, level=90,
                                  sources_to_update=None):
    """Query the GROWTH marshal for a specific science program"""
    # Get the programidx
    programidx = get_programidx(program_name)
    if programidx is None:
        return

    # Query the GROWTH Marshal
    r = requests.post(
        'http://skipper.caltech.edu:8080/cgi-bin/growth/list\
_program_sources.cgi',
        data={'programidx': str(programidx)})
    r.raise_for_status()
    sources = r.json()
    # Query the local db
    candidates = models.db.session().query(models.Candidate.name).all()
    names_all = list(candidate.name for candidate in candidates)

    if sources_to_update is not None:
        names_to_update = list(s["name"] for s in sources_to_update)
    else:
        names_to_update = []

    if new and skymap is not None:
        skymap_prob = skymap.flat_2d
        sort_idx = np.argsort(skymap_prob)[::-1]
        csm = np.empty(len(skymap_prob))
        csm[sort_idx] = np.cumsum(skymap_prob[sort_idx])
        ipix_keep = np.where(csm <= level/100.)[0]
        nside = hp.pixelfunc.get_nside(skymap_prob)

    # Add autoannotations
    for source in sources:
        if new and (source["name"] in names_all):
            continue
        if (not new) and (source["name"] not in names_to_update):
            continue
        # Skymap filter for new sources (those to update are already filtered)
        if new and skymap is not None:
            if hp.ang2pix(nside,
                          0.5 * np.pi - np.deg2rad(source["dec"]),
                          np.deg2rad(source["ra"])) not in ipix_keep:
                continue

        autoannotations_string, autoannotations_dict, photometry_marshal =\
            get_source_autoannotations_and_photometry(source["id"])

        yield dict(
            source,
            autoannotations=autoannotations_string,
            autoannotations_dict=autoannotations_dict,
            uploaded_photometry=photometry_marshal)


def update_local_db_growthmarshal(sources):
    """Takes the candidates fetched from the GROWTH marshal and
    updates the local database using SQLAlchemy."""

    sources_list = list(s for s in sources)
    name_list = list(s['name'] for s in sources_list)

    candidates = models.Candidate.query.\
        filter(models.Candidate.name.in_(name_list)).all()
    lightcurves = models.Lightcurve.query.\
        filter(models.Lightcurve.name.in_(name_list)).all()

    candidate_list = list(candidate for candidate in candidates)
    candidate_name_list = list(candidate.name for candidate in candidate_list)
    lightcurve_list = list(lc for lc in lightcurves)
    lightcurve_name_list = list(lc.name for lc in lightcurve_list)

    for s in sources_list:
        # Photometry
        s_phot_detection = list(ss for ss in s['uploaded_photometry']
                                if ss['magpsf'] < 50.)
        jd_array = list(phot['jd'] for phot in s_phot_detection)
        datetime_array = list(Time(jd_array, format='jd').datetime)
        mag_array = list(phot['magpsf'] for phot in s_phot_detection)
        magerr_array = list(phot['sigmamagpsf'] for phot in s_phot_detection)
        filt_array = list(phot['filter'] for phot in s_phot_detection)
        instrument_array = list(
                                phot['instrument']
                                for phot in s_phot_detection)

        creationdate = Time(s['creationdate'], format='iso').datetime
        lastmodified = Time(s['lastmodified'], format='iso').datetime

        try:
            rcid = int(s['rcid'])
        except (TypeError, KeyError, ValueError):
            rcid = None
        try:
            field = int(s['field'])
        except (TypeError, KeyError, ValueError):
            field = None
        try:
            candid = int(s['candid'])
        except (TypeError, KeyError, ValueError):
            candid = None
        try:
            redshift = float(s['redshift'])
        except (TypeError, KeyError, ValueError):
            redshift = None
        try:
            ps1_dr2_detections = int(
                                     s['autoannotations_dict']
                                     ['ps1_dr2_detections'])
        except (TypeError, KeyError, ValueError):
            ps1_dr2_detections = None
        try:
            gaia_match = s['autoannotations_dict']['gaia_match']
        except (TypeError, KeyError, ValueError):
            gaia_match = None
        try:
            gaia_parallax = s['autoannotations_dict']['gaia_parallax']
        except (TypeError, KeyError, ValueError):
            gaia_parallax = None
        try:
            w4 = float(s['autoannotations_dict']['w4'])
        except (TypeError, KeyError, ValueError):
            w4 = None
        try:
            w2mw3 = float(s['autoannotations_dict']['w2mw3'])
        except (TypeError, KeyError, ValueError):
            w2mw3 = None
        try:
            w1mw2 = float(s['autoannotations_dict']['w1mw2'])
        except (TypeError, KeyError, ValueError):
            w1mw2 = None
        try:
            jdstarthist =\
                Time(s['autoannotations_dict']['jdstarthist'],
                     format='jd').datetime
        except (TypeError, KeyError, ValueError):
            jdstarthist = None
        try:
            ssmagnr = float(s['autoannotations_dict']['ssmagnr'])
        except (TypeError, KeyError, ValueError):
            ssmagnr = None
        try:
            ssdistnr = float(s['autoannotations_dict']['ssdistnr'])
        except (TypeError, KeyError, ValueError):
            ssdistnr = None
        try:
            CLU_sfr_ha = float(s['autoannotations_dict']['CLU_sfr_ha'])
        except (TypeError, KeyError, ValueError):
            CLU_sfr_ha = None
        try:
            CLU_sfr_fuv = float(s['autoannotations_dict']['CLU_sfr_fuv'])
        except (TypeError, KeyError, ValueError):
            CLU_sfr_fuv = None
        try:
            CLU_z = float(s['autoannotations_dict']['CLU_z'])
        except (TypeError, KeyError, ValueError):
            CLU_z = None
        try:
            CLU_mstar = float(s['autoannotations_dict']['CLU_mstar'])
        except (TypeError, KeyError, ValueError):
            CLU_mstar = None
        try:
            CLU_d_to_galaxy_arcsec = float(
                s['autoannotations_dict']['CLU_d_to_galaxy_arcsec'])
        except (TypeError, KeyError, ValueError):
            CLU_d_to_galaxy_arcsec = None
        try:
            CLU_name = str(s['autoannotations_dict']['CLU_name'])
        except (TypeError, KeyError, ValueError):
            CLU_name = None
        try:
            CLU_id = int(s['autoannotations_dict']['CLU_id'])
        except (TypeError, KeyError, ValueError):
            CLU_id = None
        kwargs = {'name': s['name'],
                  'subfield_id': rcid,
                  'creationdate': creationdate,
                  'classification': s['classification'],
                  'redshift': redshift,
                  'iauname': s['iauname'],
                  'field_id': field,
                  'candid': candid,
                  'ra': float(s['ra']),
                  'dec': float(s['dec']),
                  'lastmodified': lastmodified,
                  'autoannotations': s['autoannotations'],
                  'ps1_dr2_detections': ps1_dr2_detections,
                  'gaia_match': gaia_match,
                  'gaia_parallax': gaia_parallax,
                  'w4': w4,
                  'w2mw3': w2mw3,
                  'w1mw2': w1mw2,
                  'jdstarthist': jdstarthist,
                  'ssmagnr': ssmagnr,
                  'ssdistnr': ssdistnr,
                  'CLU_sfr_ha': CLU_sfr_ha,
                  'CLU_mstar': CLU_mstar,
                  'CLU_sfr_fuv': CLU_sfr_fuv,
                  'CLU_z': CLU_z,
                  'CLU_name': CLU_name,
                  'CLU_id': CLU_id,
                  'CLU_d_to_galaxy_arcsec': CLU_d_to_galaxy_arcsec
                  }
        if not (s['name'] in candidate_name_list):
            models.db.session.merge(models.Candidate(**kwargs))
        else:
            candidate = candidate_list[candidate_name_list.index(s['name'])]
            merge = False
            for key in kwargs.keys():
                if kwargs[key] == getattr(candidate, key):
                    continue
                setattr(candidate, key, kwargs[key])
                merge = True
            if merge:
                models.db.session.merge(candidate)
        for date, mag, magerr, filt, instrument in zip(
                                                       datetime_array,
                                                       mag_array,
                                                       magerr_array,
                                                       filt_array,
                                                       instrument_array):
            kwargs = {'name': s['name'],
                      'date_observation': date,
                      'mag': mag,
                      'magerr': magerr,
                      'fil': filt,
                      'instrument': instrument
                      }
            if not (s['name'] in lightcurve_name_list):
                models.db.session.merge(models.Lightcurve(**kwargs))
            else:
                lightcurve = lightcurve_list[lightcurve_name_list.
                                             index(s['name'])]
                merge = False
                for key in kwargs.keys():
                    if kwargs[key] == getattr(lightcurve, key):
                        continue
                    merge = True
                    setattr(lightcurve, key, kwargs[key])
                if merge:
                    models.db.session.merge(lightcurve)
    models.db.session.commit()


def update_comment(source_name, new_comment):
    """Update the comment for a given source"""
    source = models.Candidate.query.get(source_name)
    source.comment = new_comment
    models.db.session.merge(source)
    models.db.session.commit()


@celery.task(shared=False)
def fetch_candidates_growthmarshal(new=False, dateobs=None, skymap=None,
                                   to_update=None):
    """Fetch the candidates present in the GROWTH marshal
    for the MMA science programs and store them in the local db."""

    program_names = [
        #'DECAM GW Followup',
        #'Afterglows of Fermi Gamma Ray Bursts',
        #'Electromagnetic Counterparts to Neutrinos',
        'Electromagnetic Counterparts to Gravitational Waves'
        ]
    for program_name in program_names:
        sources = get_candidates_growth_marshal(program_name, new=new,
                                                dateobs=dateobs,
                                                skymap=skymap,
                                                sources_to_update=to_update)
        update_local_db_growthmarshal(sources)
