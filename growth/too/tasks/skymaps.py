import os
from urllib.error import URLError
from urllib.parse import urlparse

from astropy.coordinates import ICRS, SkyCoord
from astropy import units as u
from astropy_healpix import HEALPix, nside_to_level, pixel_resolution_to_nside
from ligo.skymap import io
from ligo.skymap import moc
from ligo.skymap import postprocess
import numpy as np

from . import celery
from .. import models

__all__ = ('download', 'from_cone', 'contour')


@celery.task(autoretry_for=(URLError,), max_retries=20, shared=False)
def download(url, dateobs):

    def get_col(m, name):
        try:
            col = m[name]
        except KeyError:
            return None
        else:
            return col.tolist()

    filename = os.path.basename(urlparse(url).path)
    skymap = io.read_sky_map(url, moc=True)
    models.db.session.merge(
        models.Localization(
            localization_name=filename,
            dateobs=dateobs,
            uniq=get_col(skymap, 'UNIQ'),
            probdensity=get_col(skymap, 'PROBDENSITY'),
            distmu=get_col(skymap, 'DISTMU'),
            distsigma=get_col(skymap, 'DISTSIGMA'),
            distnorm=get_col(skymap, 'DISTNORM')))
    models.db.session.commit()
    return filename


@celery.task(shared=False)
def from_cone(ra, dec, error, dateobs):
    localization_name = "%.5f_%.5f_%.5f" % (ra, dec, error)

    center = SkyCoord(ra * u.deg, dec * u.deg)
    radius = error * u.deg

    # Determine resolution such that there are at least
    # 16 pixels across the error radius.
    hpx = HEALPix(pixel_resolution_to_nside(radius / 16, round='up'),
                  'nested', frame=ICRS())

    # Find all pixels in the 4-sigma error circle.
    ipix = hpx.cone_search_skycoord(center, 4 * radius)

    # Convert to multi-resolution pixel indices and sort.
    uniq = moc.nest2uniq(nside_to_level(hpx.nside), ipix)
    i = np.argsort(uniq)
    ipix = ipix[i]
    uniq = uniq[i]

    # Evaluate Gaussian.
    distance = hpx.healpix_to_skycoord(ipix).separation(center)
    probdensity = np.exp(-0.5 * np.square(distance / radius).to_value(
        u.dimensionless_unscaled))
    probdensity /= probdensity.sum() * hpx.pixel_area.to_value(u.steradian)

    models.db.session.merge(
        models.Localization(
            localization_name=localization_name,
            dateobs=dateobs,
            uniq=uniq.tolist(),
            probdensity=probdensity.tolist()))
    models.db.session.commit()

    return localization_name


@celery.task(ignore_result=True, shared=False)
def contour(localization_name, dateobs):
    localization = models.Localization.query.filter_by(
        dateobs=dateobs, localization_name=localization_name).one()

    # Calculate credible levels.
    prob = localization.flat_2d
    cls = 100 * postprocess.find_greedy_credible_levels(prob)

    # Construct contours and return as a GeoJSON feature collection.
    levels = [50, 90]
    paths = postprocess.contour(cls, levels, degrees=True, simplify=True)
    center = postprocess.posterior_max(prob)
    localization.contour = {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [center.ra.deg, center.dec.deg]
                },
                'properties': {
                    'credible_level': 0
                }
            }
        ] + [
            {
                'type': 'Feature',
                'properties': {
                    'credible_level': level
                },
                'geometry': {
                    'type': 'MultiLineString',
                    'coordinates': path
                }
            }
            for level, path in zip(levels, paths)
        ]
    }
    models.db.session.merge(localization)
    models.db.session.commit()
