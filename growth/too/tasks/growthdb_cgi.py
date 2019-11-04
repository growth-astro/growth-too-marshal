import requests
import logging

import numpy as np
import celery
from astropy.time import Time
from astropy.coordinates import SkyCoord
import astropy.units as u
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

def prepare_candidates_for_object_table(sources_all):
    """Prepare a list of disctionaries with relevant info
    for each source in the marshal"""

    sources_growth_marshal = []
    for s, l in sources_all:
        s_dict = s.__dict__
        s_dict.update(l.__dict__)
        s_dict["first_detection_time"] = l.first_detection_time
        s_dict["first_detection_mag"] = l.first_detection_mag
        s_dict["first_detection_magerr"] = l.first_detection_magerr
        s_dict["first_detection_instrument"] = l.first_detection_instrument
        s_dict["first_detection_filter"] = l.first_detection_filter
        s_dict["latest_detection_time"] = l.latest_detection_time
        s_dict["latest_detection_mag"] = l.latest_detection_mag
        s_dict["latest_detection_magerr"] = l.latest_detection_magerr
        s_dict["latest_detection_instrument"] = l.latest_detection_instrument
        s_dict["latest_detection_filter"] = l.latest_detection_filter

        if ('ra' in s_dict) and ('dec' in s_dict):
            try:
                rr, dd = s_dict['ra'].deg, s_dict['dec'].deg
            except (TypeError, KeyError, ValueError):
                rr, dd = s_dict['ra'], s_dict['dec']
            s_coords = SkyCoord(ra=rr*u.deg, dec=dd*u.deg)
            s_dict["ra"], s_dict["dec"] = s_coords.ra, s_coords.dec
            s_dict["ra_string"] = f"{'{0:02d}'.format(int(s_coords.ra.hms.h))}:\
    {'{0:02d}'.format(int(s_coords.ra.hms.m))}:\
    {'{0:02d}'.format(int(s_coords.ra.hms.s))}.\
    {'{0:02d}'.format(int(100*(s_coords.ra.hms.s - int(s_coords.ra.hms.s))))}"
            s_dict["dec_string"] = \
f"{'{0:02d}'.format(int(s_coords.dec.dms.d))}:\
    {'{0:02d}'.format(abs(int(s_coords.dec.dms.m)))}:\
    {'{0:02d}'.format(int(abs(s_coords.dec.dms.s)))}.\
    {'{0:02d}'.format(int(100*abs(s_coords.dec.dms.s - int(s_coords.dec.dms.s))))}"
        else:
            s_dict["ra_string"] = None
            s_dict["dec_string"] = None

        sources_growth_marshal.append(s_dict)

    return sources_growth_marshal


def select_sources_in_contour(sources_growth_marshal, skymap, level=90):
    """Select only those sources within a given contour
    level of the skymap"""

    skymap_prob = skymap.flat_2d
    sort_idx = np.argsort(skymap_prob)[::-1]
    csm = np.empty(len(skymap_prob))
    csm[sort_idx] = np.cumsum(skymap_prob[sort_idx])
    ipix_keep = sort_idx[np.where(csm <= level/100.)[0]]
    nside = hp.pixelfunc.get_nside(skymap_prob)
    sources_growth_marshal_contour = list(s for s in sources_growth_marshal \
if ("ra" in s) and (hp.ang2pix(nside, 0.5 * np.pi - np.deg2rad(s["dec"].value),
                    np.deg2rad(s["ra"].value)) in ipix_keep))

    return sources_growth_marshal_contour

def get_programidx(program_name):
    """Given a program name, it returns the programidx"""

    r = requests.post(
        'http://skipper.caltech.edu:8080/cgi-bin/growth/list_programs.cgi')
    r.raise_for_status()
    programs = r.json()
    program_dict = {p['name']: p['programidx'] for i, p in enumerate(programs)}

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
    autoannotations_dict = {f"{auto['type']}":auto['comment'] for auto in autoannotations}
    photometry_marshal = list(phot for phot in summary['uploaded_photometry'])

    return autoannotations_string, autoannotations_dict, photometry_marshal


def get_candidates_growth_marshal(program_name, new=False, dateobs=None,
                                  skymap=None, level=90):
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

    candidates = models.Candidate.query.all()
    names = [candidate.name for candidate in candidates]

    if not dateobs is None:
        jd_min = Time(dateobs, format='datetime').jd

    if skymap is not None:
        skymap_prob = skymap.flat_2d
        sort_idx = np.argsort(skymap_prob)[::-1]
        csm = np.empty(len(skymap_prob))
        csm[sort_idx] = np.cumsum(skymap_prob[sort_idx])
        ipix_keep = sort_idx[np.where(csm <= level/100.)[0]]
        nside = hp.pixelfunc.get_nside(skymap_prob)

    # Add autoannotations
    for source in sources:
        if new and (source["name"] in names):
            continue
        if (not new) and (source["name"] not in names):
            continue
        if not skymap is None:
            if hp.ang2pix(nside,
                          0.5 * np.pi - np.deg2rad(source["dec"]),
                          np.deg2rad(source["ra"])) not in ipix_keep:
                continue

        if dateobs is not None:
            autoannotations_string, autoannotations_dict, photometry_marshal =\
 get_source_autoannotations_and_photometry(source["id"])
            s_phot_detection = list(ss for ss in photometry_marshal if ss['magpsf'] < 50.)
            jd_array = np.array(list(phot['jd'] for phot in s_phot_detection))
            if jd_min > np.min(jd_array):
                continue

        yield dict(
            source,
            autoannotations=autoannotations_string,
            autoannotations_dict=autoannotations_dict,
            uploaded_photometry=photometry_marshal)


def get_or_create(session, model, defaults=None, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        params = dict((k, v) for k, v in kwargs.iteritems() if not isinstance(v, ClauseElement))
        params.update(defaults or {})
        instance = model(**params)
        session.add(instance)
        return instance, True


def update_local_db_growthmarshal(sources):
    """Takes the candidates fetched from the GROWTH marshal and
    updates the local database using SQLAlchemy."""

    for s in sources:
        # Photometry
        s_phot_detection = list(
                                ss for ss in s['uploaded_photometry'] if ss['magpsf'] < 50.)
        jd_array = list(phot['jd'] for phot in s_phot_detection)
        datetime_array = list(Time(jd_array, format='jd').datetime)
        mag_array = list(phot['magpsf'] for phot in s_phot_detection)
        magerr_array = list(phot['sigmamagpsf'] for phot in s_phot_detection)
        filt_array = list(phot['filter'] for phot in s_phot_detection)
        instrument_array = list(
                                phot['instrument'] for phot in s_phot_detection)
        min_jd = min(jd_array)

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
            ps1_dr2_detections = int(s['autoannotations_dict']['ps1_dr2_detections'])
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
            jdstarthist = Time(s['autoannotations_dict']['jdstarthist'], format='jd').datetime
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
            CLU_d_to_galaxy_arcsec = float(s['autoannotations_dict']['CLU_d_to_galaxy_arcsec'])
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
        candidate = models.Candidate.query.filter_by(name=s['name']).all()
        if len(candidate) == 0:
            models.db.session.merge(models.Candidate(**kwargs))
        else:
            candidate = candidate[0]
            merge = False
            for key in kwargs.keys():
                if kwargs[key] == getattr(candidate, key):
                    continue
                setattr(candidate, key, kwargs[key])
                merge = True
            if merge:
                models.db.session.merge(candidate)

        kwargs = {'name': s['name'],
                  'date_observation': datetime_array,
                  'mag': mag_array,
                  'magerr': magerr_array,
                  'fil': filt_array,
                  'instrument': instrument_array,
                  'first_detection_time_tmp': Time(min_jd,
                                                   format='jd').datetime
                 }
        lightcurve = models.Lightcurve.query.filter_by(name=s['name']).all()
        if len(lightcurve) == 0:
            models.db.session.merge(models.Lightcurve(**kwargs))
        else:
            lightcurve = lightcurve[0]
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
    source = models.Candidate.query.filter_by(name=source_name).first()
    source.comment = new_comment
    models.db.session.merge(source)
    models.db.session.commit()


@celery.task(shared=False)
def fetch_candidates_growthmarshal(new=False, dateobs=None, skymap=None):
    """Fetch the candidates present in the GROWTH marshal
    for the MMA science programs and store them in the local db."""

    program_names = [
        'DECAM GW Followup',
        'Afterglows of Fermi Gamma Ray Bursts',
        'Electromagnetic Counterparts to Neutrinos',
        'Electromagnetic Counterparts to Gravitational Waves'
        ]
    for program_name in program_names:
        sources = get_candidates_growth_marshal(program_name, new=new,
                                                dateobs=dateobs,
                                                skymap=skymap)
        update_local_db_growthmarshal(sources)
