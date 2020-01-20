"""
Database schema.
"""

import datetime
import enum
import os
import copy
import gwemopt.utils
import gwemopt.ztf_tiling

from astropy import table
from astropy import coordinates
from astropy import units as u
from astropy.time import Time
from flask_login.mixins import UserMixin
from flask_sqlalchemy import SQLAlchemy
import gcn
import healpy as hp
from ligo.skymap.bayestar import rasterize
import lxml.etree
import pkg_resources
import numpy as np
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy_utils import EmailType, PhoneNumberType
from tqdm import tqdm

from .flask import app

db = SQLAlchemy(app)


def get_ztf_quadrants():
    """Calculate ZTF quadrant footprints as offsets from the telescope
    boresight."""
    quad_prob = gwemopt.ztf_tiling.QuadProb(0, 0)
    ztf_tile = gwemopt.ztf_tiling.ZTFtile(0, 0)
    quad_cents_ra, quad_cents_dec = ztf_tile.quadrant_centers()
    offsets = np.asarray([
        quad_prob.getWCS(
            quad_cents_ra[quadrant_id],
            quad_cents_dec[quadrant_id]
        ).calc_footprint(axes=quad_prob.quadrant_size)
        for quadrant_id in range(64)])
    return np.transpose(offsets, (2, 0, 1))


def create_all():
    db.create_all(bind=None)
    telescopes = ["ZTF", "Gattini", "DECam", "KPED", "GROWTH-India"]
    available_filters = {"ZTF": ["g", "r", "i"],
                         "Gattini": ["J"],
                         "DECam": ["g", "r", "i", "z"],
                         "KPED": ["U", "g", "r", "i"],
                         "GROWTH-India": ["g", "r", "i", "z"]}

    plan_args = {
        'ZTF': {
            'filt': ['g', 'r', 'g'],
            'exposuretimes': [300.0, 300.0, 300.0],
            'doReferences': True,
            'doUsePrimary': True,
            'doBalanceExposure': False,
            'doDither': False,
            'usePrevious': False,
            'doCompletedObservations': False,
            'doPlannedObservations': False,
            'cobs': [None, None],
            'schedule_type': 'greedy',
            'filterScheduleType': 'block',
            'airmass': 2.5,
            'schedule_strategy': 'tiling',
            'mindiff': 30.*60.,
            'doMaxTiles': False,
            'max_nb_tiles': 1000,
            'doRASlice': True,
            'raslice': [0.0, 24.0],
        },
        'DECam': {
            'filt': ['g', 'z'],
            'exposuretimes': [25.0, 25.0],
            'doReferences': True,
            'doUsePrimary': False,
            'doBalanceExposure': False,
            'doDither': True,
            'usePrevious': False,
            'doCompletedObservations': False,
            'doPlannedObservations': False,
            'cobs': [None, None],
            'schedule_type': 'greedy_slew',
            'filterScheduleType': 'integrated',
            'airmass': 2.5,
            'schedule_strategy': 'tiling',
            'mindiff': 30.*60.,
            'doMaxTiles': False,
            'max_nb_tiles': 1000,
            'doRASlice': True,
            'raslice': [0.0, 24.0],
        },
        'Gattini': {
            'filt': ['J'],
            'exposuretimes': [300.0],
            'doReferences': False,
            'doUsePrimary': False,
            'doBalanceExposure': False,
            'doDither': False,
            'usePrevious': False,
            'doCompletedObservations': False,
            'doPlannedObservations': False,
            'cobs': [None, None],
            'schedule_type': 'greedy',
            'filterScheduleType': 'block',
            'airmass': 2.5,
            'schedule_strategy': 'tiling',
            'mindiff': 30.*60.,
            'doMaxTiles': False,
            'max_nb_tiles': 1000,
            'doRASlice': True,
            'raslice': [0.0, 24.0]
        },
        'KPED': {
            'filt': ['r'],
            'exposuretimes': [300.0],
            'doReferences': False,
            'doUsePrimary': False,
            'doBalanceExposure': False,
            'doDither': False,
            'usePrevious': False,
            'doCompletedObservations': False,
            'doPlannedObservations': False,
            'cobs': [None, None],
            'schedule_type': 'greedy',
            'filterScheduleType': 'integrated',
            'airmass': 2.5,
            'schedule_strategy': 'catalog',
            'mindiff': 30.*60.,
            'doMaxTiles': False,
            'max_nb_tiles': 1000,
            'doRASlice': True,
            'raslice': [0.0, 24.0]
        },
        'GROWTH-India': {
            'filt': ['r'],
            'exposuretimes': [300.0],
            'doReferences': False,
            'doUsePrimary': False,
            'doBalanceExposure': False,
            'doDither': False,
            'usePrevious': False,
            'doCompletedObservations': False,
            'doPlannedObservations': False,
            'cobs': [None, None],
            'schedule_type': 'greedy',
            'filterScheduleType': 'integrated',
            'airmass': 2.5,
            'schedule_strategy': 'catalog',
            'mindiff': 30.*60.,
            'doMaxTiles': False,
            'max_nb_tiles': 1000,
            'doRASlice': True,
            'raslice': [0.0, 24.0]
        }
    }

    with tqdm(telescopes) as telescope_progress:
        for tele in telescope_progress:
            telescope_progress.set_description('populating {}'.format(tele))

            filename = pkg_resources.resource_filename(
                __name__, 'input/%s.ref' % tele)
            if os.path.isfile(filename):
                refstable = table.Table.read(
                    filename, format='ascii', data_start=2, data_end=-1)
                refs = table.unique(refstable, keys=['field', 'fid'])
                if "maglimcat" not in refs.columns:
                    refs["maglimcat"] = np.nan

                reference_images = {
                    group[0]['field']: group['fid'].astype(int).tolist()
                    for group in refs.group_by('field').groups}
                reference_mags = {
                    group[0]['field']: group['maglimcat'].tolist()
                    for group in refs.group_by('field').groups}

            else:
                reference_images = {}
                reference_mags = {}

            tesspath = 'input/%s.tess' % tele
            try:
                tessfile = app.open_instance_resource(tesspath)
            except IOError:
                tessfile = pkg_resources.resource_stream(__name__, tesspath)
            tessfilename = tessfile.name
            tessfile.close()
            fields = np.recfromtxt(
                tessfilename, usecols=range(3),
                names=['field_id', 'ra', 'dec'])

            with pkg_resources.resource_stream(
                    __name__, 'config/%s.config' % tele) as g:
                config_struct = {}
                for line in g.readlines():
                    line_without_return = line.decode().split("\n")
                    line_split = line_without_return[0].split(" ")
                    line_split = list(filter(None, line_split))
                    if line_split:
                        try:
                            config_struct[line_split[0]] = float(line_split[1])
                        except ValueError:
                            config_struct[line_split[0]] = line_split[1]

            db.session.merge(Telescope(telescope=tele,
                                       lat=config_struct["latitude"],
                                       lon=config_struct["longitude"],
                                       elevation=config_struct["elevation"],
                                       timezone=config_struct["timezone"],
                                       filters=available_filters[tele],
                                       default_plan_args=plan_args[tele]))

            for field_id, ra, dec in tqdm(fields, 'populating fields'):
                ref_filter_ids = reference_images.get(field_id, [])
                ref_filter_mags = []
                for val in reference_mags.get(field_id, []):
                    ref_filter_mags.append(val)
                bands = {1: 'g', 2: 'r', 3: 'i', 4: 'z', 5: 'J'}
                ref_filter_bands = [bands.get(n, n) for n
                                    in ref_filter_ids]

                if config_struct["FOV_type"] == "square":
                    ipix, radecs, patch, area = gwemopt.utils.getSquarePixels(
                        ra, dec, config_struct["FOV"], Localization.nside)
                elif config_struct["FOV_type"] == "circle":
                    ipix, radecs, patch, area = gwemopt.utils.getCirclePixels(
                        ra, dec, config_struct["FOV"], Localization.nside)
                if len(radecs) == 0:
                    continue
                corners = np.vstack((radecs, radecs[0, :]))
                if corners.size == 10:
                    corners_copy = copy.deepcopy(corners)
                    corners[2] = corners_copy[3]
                    corners[3] = corners_copy[2]
                contour = {
                    'type': 'Feature',
                    'geometry': {
                        'type': 'MultiLineString',
                        'coordinates': [corners.tolist()]
                    },
                    'properties': {
                        'telescope': tele,
                        'field_id': int(field_id),
                        'ra': ra,
                        'dec': dec,
                        'depth': dict(zip(ref_filter_bands, ref_filter_mags))
                    }
                }
                db.session.merge(Field(telescope=tele,
                                       field_id=int(field_id),
                                       ra=ra, dec=dec, contour=contour,
                                       reference_filter_ids=ref_filter_ids,
                                       reference_filter_mags=ref_filter_mags,
                                       ipix=ipix.tolist()))

            if tele == "ZTF":
                quadrant_coords = get_ztf_quadrants()

                skyoffset_frames = coordinates.SkyCoord(
                    fields['ra'], fields['dec'], unit=u.deg
                ).skyoffset_frame()

                quadrant_coords_icrs = coordinates.SkyCoord(
                    *np.tile(
                        quadrant_coords[:, np.newaxis, ...],
                        (len(fields), 1, 1)), unit=u.deg,
                    frame=skyoffset_frames[:, np.newaxis, np.newaxis]
                ).transform_to(coordinates.ICRS)

                quadrant_xyz = np.moveaxis(
                    quadrant_coords_icrs.cartesian.xyz.value, 0, -1)

                for field_id, xyz in zip(
                        tqdm(fields['field_id'], 'populating subfields'),
                        quadrant_xyz):
                    for ii, xyz in enumerate(xyz):
                        ipix = hp.query_polygon(Localization.nside, xyz)
                        db.session.merge(SubField(telescope=tele,
                                                  field_id=int(field_id),
                                                  subfield_id=int(ii),
                                                  ipix=ipix.tolist()))


class User(db.Model, UserMixin):

    name = db.Column(
        db.String,
        primary_key=True,
        comment='Unique username')

    email = db.Column(
        EmailType,
        comment='E-mail address')

    phone = db.Column(
        PhoneNumberType,
        comment='Mobile/SMS phone number')

    voice = db.Column(
        db.Boolean,
        nullable=False,
        default=False,
        comment='Set to true for voice alerts (default: SMS only)')

    timezone = db.Column(
        db.Unicode,
        nullable=False,
        default='America/New_York')

    alert_from = db.Column(
        db.Time,
        comment='Start of hours for alerts')

    alert_to = db.Column(
        db.Time,
        comment='End of hours for alerts')

    def get_id(self):
        """Provide user ID for flask_login."""
        return self.name


class Event(db.Model):
    """Event information, including an event ID, mission, and time of the
    event"""

    dateobs = db.Column(
        db.DateTime,
        primary_key=True,
        comment='Event time')

    gcn_notices = db.relationship(
        lambda: GcnNotice,
        order_by=lambda: GcnNotice.date)

    _tags = db.relationship(
        lambda: Tag,
        lazy='selectin',
        order_by=lambda: (
            db.func.lower(Tag.text).notin_({'fermi', 'swift', 'amon', 'lvc'}),
            db.func.lower(Tag.text).notin_({'long', 'short'}),
            db.func.lower(Tag.text).notin_({'grb', 'gw', 'transient'})
        )
    )

    tags = association_proxy(
        '_tags',
        'text',
        creator=lambda tag: Tag(text=tag))

    localizations = db.relationship(lambda: Localization)

    plans = db.relationship(lambda: Plan, backref='event')

    @hybrid_property
    def retracted(self):
        return 'retracted' in self.tags

    @retracted.expression
    def retracted(cls):
        return db.literal('retracted').in_(cls.tags)

    @property
    def lightcurve(self):
        try:
            notice = self.gcn_notices[0]
        except IndexError:
            return None
        root = lxml.etree.fromstring(notice.content)
        elem = root.find(".//Param[@name='LightCurve_URL']")
        if elem is None:
            return None
        else:
            return elem.attrib.get('value', '').replace('http://', 'https://')

    @property
    def gracedb(self):
        try:
            notice = self.gcn_notices[0]
        except IndexError:
            return None
        root = lxml.etree.fromstring(notice.content)
        elem = root.find(".//Param[@name='EventPage']")
        if elem is None:
            return None
        else:
            return elem.attrib.get('value', '')

    @property
    def ned_gwf(self):
        return "https://ned.ipac.caltech.edu/gwf/events"

    @property
    def graceid(self):
        try:
            notice = self.gcn_notices[0]
        except IndexError:
            return None
        root = lxml.etree.fromstring(notice.content)
        elem = root.find(".//Param[@name='GraceID']")
        if elem is None:
            return None
        else:
            return elem.attrib.get('value', '')


class Tag(db.Model):
    """Store qualitative tags for events."""

    dateobs = db.Column(
        db.DateTime,
        db.ForeignKey(Event.dateobs),
        primary_key=True)

    text = db.Column(
        db.Unicode,
        primary_key=True)


class Telescope(db.Model):
    """Telescope information"""

    telescope = db.Column(
        db.String,
        primary_key=True,
        comment='Telescope name')

    lat = db.Column(
        db.Float,
        nullable=False,
        comment='Latitude')

    lon = db.Column(
        db.Float,
        nullable=False,
        comment='Longitude')

    elevation = db.Column(
        db.Float,
        nullable=False,
        comment='Elevation')

    timezone = db.Column(
        db.String,
        nullable=False,
        comment='Time zone')

    filters = db.Column(
        db.ARRAY(db.String),
        nullable=False,
        comment='Available filters')

    fields = db.relationship(lambda: Field)

    plans = db.relationship(lambda: Plan)

    default_plan_args = db.Column(
        db.JSON,
        nullable=False,
        comment='Default plan arguments')


class Field(db.Model):
    """Footprints and number of observations in each filter for standard PTF
    tiles"""

    telescope = db.Column(
        db.String,
        db.ForeignKey(Telescope.telescope),
        primary_key=True,
        comment='Telescope')

    field_id = db.Column(
        db.Integer,
        primary_key=True,
        comment='Field ID')

    ra = db.Column(
        db.Float,
        nullable=False,
        comment='RA of field center')

    dec = db.Column(
        db.Float,
        nullable=False,
        comment='Dec of field center')

    contour = db.Column(
        db.JSON,
        nullable=False,
        comment='GeoJSON contours')

    reference_filter_ids = db.Column(
        db.ARRAY(db.Integer),
        nullable=False,
        comment='Reference filter IDs')

    reference_filter_mags = db.Column(
        db.ARRAY(db.Float),
        nullable=False,
        comment='Reference filter magss')

    ipix = db.Column(
        db.ARRAY(db.Integer),
        comment='Healpix indices')

    subfields = db.relationship(lambda: SubField)


class SubField(db.Model):
    """SubFields"""

    __table_args__ = (
        db.ForeignKeyConstraint(
            ['telescope',
             'field_id'],
            ['field.telescope',
             'field.field_id']
        ),
    )

    telescope = db.Column(
        db.String,
        db.ForeignKey(Telescope.telescope),
        primary_key=True,
        comment='Telescope')

    field_id = db.Column(
        db.Integer,
        primary_key=True,
        comment='Field ID')

    subfield_id = db.Column(
        db.Integer,
        primary_key=True,
        comment='SubField ID')

    ipix = db.Column(
        db.ARRAY(db.Integer),
        comment='Healpix indices')


class GcnNotice(db.Model):
    """Records of ingested GCN notices"""

    ivorn = db.Column(
        db.String,
        primary_key=True,
        comment='Unique identifier of VOEvent')

    notice_type = db.Column(
        db.Enum(gcn.NoticeType, native_enum=False),
        nullable=False,
        comment='GCN Notice type')

    stream = db.Column(
        db.String,
        nullable=False,
        comment='Event stream or mission (i.e., "Fermi")')

    date = db.Column(
        db.DateTime,
        nullable=False,
        comment='UTC message timestamp')

    dateobs = db.Column(
        db.DateTime,
        db.ForeignKey(Event.dateobs),
        nullable=False,
        comment='UTC event timestamp')

    content = db.deferred(db.Column(
        db.LargeBinary,
        nullable=False,
        comment='Raw VOEvent content'))

    def _get_property(self, property_name, value=None):
        root = lxml.etree.fromstring(self.content)
        path = ".//Param[@name='{}']".format(property_name)
        elem = root.find(path)
        if elem is None:
            return None
        value = float(elem.attrib.get('value', '')) * 100
        return value

    @property
    def has_ns(self):
        return self._get_property(property_name="HasNS")

    @property
    def has_remnant(self):
        return self._get_property(property_name="HasRemnant")

    @property
    def far(self):
        return self._get_property(property_name="FAR")

    @property
    def bns(self):
        return self._get_property(property_name="BNS")

    @property
    def nsbh(self):
        return self._get_property(property_name="NSBH")

    @property
    def bbh(self):
        return self._get_property(property_name="BBH")

    @property
    def mass_gap(self):
        return self._get_property(property_name="MassGap")

    @property
    def noise(self):
        return self._get_property(property_name="Terrestrial")


class Localization(db.Model):
    """Localization information, including the localization ID, event ID, right
    ascension, declination, error radius (if applicable), and the healpix
    map."""

    nside = 512
    """HEALPix resolution used for flat (non-multiresolution) operations."""

    dateobs = db.Column(
        db.DateTime,
        db.ForeignKey(Event.dateobs),
        primary_key=True,
        comment='UTC event timestamp')

    localization_name = db.Column(
        db.String,
        primary_key=True,
        comment='Localization name')

    uniq = db.deferred(db.Column(
        db.ARRAY(db.BigInteger),
        nullable=False,
        comment='Multiresolution HEALPix UNIQ pixel index array'))

    probdensity = db.deferred(db.Column(
        db.ARRAY(db.Float),
        nullable=False,
        comment='Multiresolution HEALPix probability density array'))

    distmu = db.deferred(db.Column(
        db.ARRAY(db.Float),
        comment='Multiresolution HEALPix distance mu array'))

    distsigma = db.deferred(db.Column(
        db.ARRAY(db.Float),
        comment='Multiresolution HEALPix distance sigma array'))

    distnorm = db.deferred(db.Column(
        db.ARRAY(db.Float),
        comment='Multiresolution HEALPix distance normalization array'))

    contour = db.deferred(db.Column(
        db.JSON,
        comment='GeoJSON contours'))

    @hybrid_property
    def is_3d(self):
        return (self.distmu is not None and
                self.distsigma is not None and
                self.distnorm is not None)

    @is_3d.expression
    def is_3d(self):
        return (self.distmu.isnot(None) and
                self.distsigma.isnot(None) and
                self.distnorm.isnot(None))

    @property
    def table_2d(self):
        """Get multiresolution HEALPix dataset, probability density only."""
        return table.Table(
            [self.uniq, self.probdensity], names=['UNIQ', 'PROBDENSITY'])

    @property
    def table(self):
        """Get multiresolution HEALPix dataset, probability density and
        distance."""
        if self.is_3d:
            return table.Table(
                [
                    self.uniq,
                    self.probdensity, self.distmu,
                    self.distsigma, self.distnorm],
                names=[
                    'UNIQ', 'PROBDENSITY', 'DISTMU', 'DISTSIGMA', 'DISTNORM'])
        else:
            return self.table_2d

    @property
    def flat_2d(self):
        """Get flat resolution HEALPix dataset, probability density only."""
        order = hp.nside2order(Localization.nside)
        result = rasterize(self.table_2d, order)['PROB']
        return hp.reorder(result, 'NESTED', 'RING')

    @property
    def flat(self):
        """Get flat resolution HEALPix dataset, probability density and
        distance."""
        if self.is_3d:
            order = hp.nside2order(Localization.nside)
            t = rasterize(self.table, order)
            result = t['PROB'], t['DISTMU'], t['DISTSIGMA'], t['DISTNORM']
            return hp.reorder(result, 'NESTED', 'RING')
        else:
            return self.flat_2d,


class Plan(db.Model):
    """Tiling information, including the event time, localization ID, tile IDs,
    and plan name"""

    dateobs = db.Column(
        db.DateTime,
        db.ForeignKey(Event.dateobs),
        primary_key=True,
        comment='UTC event timestamp')

    telescope = db.Column(
        db.String,
        db.ForeignKey(Telescope.telescope),
        primary_key=True,
        comment='Telescope')

    plan_name = db.Column(
        db.String,
        primary_key=True,
        comment='Plan name')

    validity_window_start = db.Column(
        db.DateTime,
        nullable=False,
        default=lambda: datetime.datetime.now(),
        comment='Start of validity window')

    validity_window_end = db.Column(
        db.DateTime,
        nullable=False,
        default=lambda: datetime.datetime.now() + datetime.timedelta(1),
        comment='End of validity window')

    plan_args = db.Column(
        db.JSON,
        nullable=False,
        comment='Plan arguments')

    # FIXME: Hard-code program_id, filter_id, subprogram_name
    program_id = 2

    class Status(enum.IntEnum):
        WORKING = 0
        READY = 1
        SUBMITTED = 2

    status = db.Column(
        db.Enum(Status),
        default=Status.WORKING,
        nullable=False,
        comment='Plan status')

    planned_observations = db.relationship(
        'PlannedObservation', backref='plan',
        order_by=lambda: PlannedObservation.obstime)

    @property
    def start_observation(self):
        """Time of the first planned observation."""
        if self.planned_observations:
            return self.planned_observations[0].obstime
        else:
            return None

    @property
    def end_observation(self):
        """Time of the end of planned observations."""
        if self.planned_observations:
            lastexp = self.planned_observations[-1]
            end = Time(lastexp.obstime) + \
                (lastexp.exposure_time + lastexp.overhead_per_exposure) * u.s
            return end.datetime
        else:
            return None

    @hybrid_property
    def num_observations(self):
        """Number of planned observation."""
        return len(self.planned_observations)

    @num_observations.expression
    def num_observations(cls):
        """Number of planned observation."""
        return cls.planned_observations.count()

    @property
    def num_observations_per_filter(self):
        """Number of planned observation per filter."""
        filters = list(Telescope.query.get(self.telescope).filters)
        nepochs = np.zeros(len(filters),)
        bands = {1: 'g', 2: 'r', 3: 'i', 4: 'z', 5: 'J'}
        for planned_observation in self.planned_observations:
            filt = bands[planned_observation.filter_id]
            idx = filters.index(filt)
            nepochs[idx] = nepochs[idx] + 1
        nobs_per_filter = []
        for ii, filt in enumerate(filters):
            nobs_per_filter.append("%s: %d" % (filt, nepochs[ii]))
        return " ".join(nobs_per_filter)

    @property
    def total_time(self):
        """Total observation time (seconds)."""
        return sum(_.exposure_time for _ in self.planned_observations)

    @property
    def tot_time_with_overheads(self):
        overhead = sum(
            _.overhead_per_exposure for _ in self.planned_observations)
        return overhead + self.total_time

    @property
    def ipix(self):
        return {
            i for planned_observation in self.planned_observations
            if planned_observation.field.ipix is not None
            for i in planned_observation.field.ipix}

    @property
    def area(self):
        nside = Localization.nside
        return hp.nside2pixarea(nside, degrees=True) * len(self.ipix)

    def get_probability(self, localization):
        ipix = np.asarray(list(self.ipix))
        if len(ipix) > 0:
            return localization.flat_2d[ipix].sum()
        else:
            return 0.0


class PlannedObservation(db.Model):
    """Tile information, including the event time, localization ID, field IDs,
    tiling name, and tile probabilities."""

    __table_args__ = (
        db.ForeignKeyConstraint(
            ['dateobs',
             'telescope',
             'plan_name'],
            ['plan.dateobs',
             'plan.telescope',
             'plan.plan_name'],
            ondelete='CASCADE',
            onupdate='CASCADE'
        ),
        db.ForeignKeyConstraint(
            ['telescope',
             'field_id'],
            ['field.telescope',
             'field.field_id']
        ),
    )

    planned_observation_id = db.Column(
        db.Integer,
        primary_key=True,
        comment='Exposure ID')

    dateobs = db.Column(
        db.DateTime,
        db.ForeignKey(Event.dateobs),
        primary_key=True,
        comment='UTC event timestamp')

    telescope = db.Column(
        db.String,
        db.ForeignKey(Telescope.telescope),
        primary_key=True,
        comment='Telescope')

    field_id = db.Column(
        db.Integer,
        primary_key=True,
        comment='Field ID')

    plan_name = db.Column(
        db.String,
        primary_key=True,
        comment='Plan name')

    field = db.relationship(Field, viewonly=True)

    exposure_time = db.Column(
        db.Integer,
        nullable=False,
        comment='Exposure time in seconds')

    # FIXME: remove
    weight = db.Column(
        db.Float,
        nullable=False,
        comment='Weight associated with each observation')

    filter_id = db.Column(
        db.Integer,
        nullable=False,
        comment='Filter ID (g=1, r=2, i=3, z=4, J=5)')

    obstime = db.Column(
        db.DateTime,
        nullable=False,
        comment='UTC observation timestamp')

    overhead_per_exposure = db.Column(
        db.Integer,
        nullable=False,
        comment='Overhead time per exposure in seconds')


class Observation(db.Model):
    """Observation information, including the field ID, exposure time, and
    filter."""

    __table_args__ = (
        db.ForeignKeyConstraint(
            ['telescope',
             'field_id'],
            ['field.telescope',
             'field.field_id']
        ),
    )

    telescope = db.Column(
        db.String,
        db.ForeignKey(Telescope.telescope),
        primary_key=True,
        comment='Telescope')

    field_id = db.Column(
        db.Integer,
        primary_key=True,
        comment='Field ID')

    observation_id = db.Column(
        db.Integer,
        primary_key=True,
        comment='Observation ID')

    obstime = db.Column(
        db.DateTime,
        comment='Exposure timestamp')

    field = db.relationship(Field)

    filter_id = db.Column(
        db.Integer,
        nullable=False,
        comment='Filter ID (g=1, r=2, i=3, z=4, J=5)')

    exposure_time = db.Column(
        db.Integer,
        nullable=False,
        comment='Exposure times')

    airmass = db.Column(
        db.Float,
        comment='Airmass')

    seeing = db.Column(
        db.Float,
        comment='Seeing')

    limmag = db.Column(
        db.Float,
        comment='Limiting magnitude')

    subfield_id = db.Column(
        db.Integer,
        default=0,
        primary_key=True,
        nullable=False,
        comment='subfield (e.g. quadrant/chip as relevant for instrument')

    successful = db.Column(
        db.Boolean,
        nullable=False,
        comment='processed successfully?')


class Candidate(db.Model):

    name = db.Column(
        db.String,
        primary_key=True,
        comment='Candidate name')

    growth_marshal_id = db.Column(
        db.String,
        unique=True, nullable=False,
        comment='GROWTH marshal ID')

    subfield_id = db.Column(
        db.Integer,
        nullable=True,
        comment='Readout channel ID')

    creationdate = db.Column(
        db.DateTime,
        comment='Date of candidate creation')

    classification = db.Column(
        db.String,
        nullable=True,
        comment='Classification')

    redshift = db.Column(
        db.Float,
        nullable=True,
        comment='Resdshift of the source')

    iauname = db.Column(
        db.String,
        nullable=True,
        comment='IAU name on TNS')

    field_id = db.Column(
        db.Integer,
        comment='Field ID')

    candid = db.Column(
        db.BigInteger,
        comment='Candidate ID')

    ra = db.Column(
        db.Float,
        nullable=False,
        comment='RA of the candidate')

    dec = db.Column(
        db.Float,
        nullable=False,
        comment='Dec of the candidate')

    last_updated = db.Column(
        db.DateTime,
        nullable=False,
        comment='Date of last update')

    autoannotations = db.Column(
        db.String,
        nullable=True,
        comment='Autoannotations from the GROWTH marshal')

    photometry = db.relationship(
        lambda: CandidatePhotometry,
        backref='candidate',
        order_by=lambda: CandidatePhotometry.dateobs)

    @property
    def first_detection_time(self):
        try:
            return self.photometry[0].dateobs
        except IndexError:
            return None

    @property
    def first_detection_mag(self):
        try:
            return self.photometry[0].mag
        except IndexError:
            return None

    @property
    def first_detection_magerr(self):
        try:
            return self.photometry[0].magerr
        except IndexError:
            return None

    @property
    def first_detection_instrument(self):
        try:
            return self.photometry[0].instrument
        except IndexError:
            return None

    @property
    def first_detection_filter(self):
        try:
            return self.photometry[0].fil
        except IndexError:
            return None

    @property
    def last_detection_time(self):
        try:
            return self.photometry[-1].dateobs
        except IndexError:
            return None

    @property
    def last_detection_mag(self):
        try:
            return self.photometry[-1].mag
        except IndexError:
            return None

    @property
    def last_detection_magerr(self):
        try:
            return self.photometry[-1].magerr
        except IndexError:
            return None

    @property
    def last_detection_instrument(self):
        try:
            return self.photometry[-1].instrument
        except IndexError:
            return None

    @property
    def last_detection_filter(self):
        try:
            return self.photometry[-1].fil
        except IndexError:
            return None


class CandidatePhotometry(db.Model):
    """Candidate light curve pulled from the GROWTH
    Marshal"""

    lcid = db.Column(
        db.BigInteger,
        primary_key=True)

    name = db.Column(
        db.ForeignKey(Candidate.name),
        nullable=False,
        comment='Candidate name')

    dateobs = db.Column(
        db.DateTime,
        nullable=True,
        index=True,
        comment='Observation date')

    fil = db.Column(
        db.String,
        nullable=True,
        comment='Filter')

    instrument = db.Column(
        db.String,
        nullable=True,
        comment='Instruments')

    limmag = db.Column(
        db.Float,
        nullable=True,
        comment='Limiting magnitude')

    mag = db.Column(
        db.Float,
        nullable=True,
        comment='Mag PSF')

    magerr = db.Column(
        db.Float,
        nullable=True,
        comment='Mag uncertainty')

    exptime = db.Column(
        db.Float,
        nullable=True,
        comment='Exposure time')

    programid = db.Column(
        db.Integer,
        nullable=True,
        comment='Program ID number (1,2,3)')
