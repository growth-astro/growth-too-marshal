import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy.dialects.postgresql import array

from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker, scoped_session, relationship

import numpy as np

from sqlalchemy.ext.automap import automap_base

import time
import os
import pandas as pd

from astropy.wcs import WCS
from astropy.io import fits
from astropy.coordinates import SkyCoord

from astropy.table import Table

import gzip
import io
import boto3
from botocore.exceptions import ClientError, CredentialRetrievalError
from botocore.exceptions import MetadataRetrievalError, NoCredentialsError
import json

import requests

from datetime import datetime

AutoBase = automap_base()

DBSession = scoped_session(sessionmaker())

# The db has to be initialized later; this is done by the app itself
# See `app_server.py`


def get_secret(key):
    secret_name = "marshaldb"
    region_name = "us-east-1"

    # Create a Secrets Manager client

    while True:
        try:
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=region_name
            )
        except (CredentialRetrievalError,
                MetadataRetrievalError, NoCredentialsError):
            print('encountered AWS ratelimit, retrying...')
            continue

    # In this sample we only handle the specific exceptions
    # for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/
    # latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except (CredentialRetrievalError,
                MetadataRetrievalError, NoCredentialsError):
            print('encountered AWS ratelimit, retrying...')
            continue
        except ClientError as e:
            ecode = e.response['Error']['Code']
            if ecode == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected
                # secret text using the provided KMS key.
                # Deal with the exception here, and/or
                # rethrow at your discretion.
                raise e
            elif ecode == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or
                # rethrow at your discretion.
                raise e
            elif ecode == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or
                # rethrow at your discretion.
                raise e
            elif ecode == 'InvalidRequestException':
                # You provided a parameter value that is not valid
                # for the current state of the resource.
                # Deal with the exception here, and/or
                # rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or
                # rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary,
            # one of these fields will be populated.
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)[key]


def to_bytes(fname):
    with fits.open(fname) as f:
        stamp_array = f[0].data

    fitsbuf = io.BytesIO()
    gzbuf = io.BytesIO()

    fits.writeto(fitsbuf, stamp_array)
    with gzip.open(gzbuf, 'wb') as fz:
        fz.write(fitsbuf.getvalue())
    return gzbuf.getvalue()


def jpg_to_bytes(fname):
    gzbuf = io.BytesIO()
    with gzip.open(gzbuf, 'wb') as gz, open(fname, 'rb') as f:
        gz.write(bytes(f.read()))
    return gzbuf.getvalue()


def create_tables(retry=5):
    """
    Create tables for all models, retrying 5 times at intervals of 3
    seconds if the database is not reachable.
    """
    for i in range(1, retry + 1):
        try:
            conn = DBSession.session_factory.kw['bind']
            print(f'Creating tables on database {conn.url.database}')
            Base.metadata.create_all()

            print('Refreshed tables:')
            for m in Base.metadata.tables:
                print(f' - {m}')

            return

        except Exception as e:
            if (i == retry):
                raise e
            else:
                print('Could not connect to database...sleeping 3')
                print(f'  > {e}')
                time.sleep(3)


data_types = {
    int: 'int',
    float: 'float',
    bool: 'bool',
    dict: 'dict',
    str: 'str',
    list: 'list'
}


class Encoder(json.JSONEncoder):
    """Extends json.JSONEncoder with additional capabilities/configurations."""

    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        elif isinstance(o, bytes):
            return o.decode('utf-8')

        elif hasattr(o, '__table__'):  # SQLAlchemy model
            return o.to_dict()

        elif o is int:
            return 'int'

        elif o is float:
            return 'float'

        elif type(o).__name__ == 'ndarray':  # avoid numpy import
            return o.tolist()

        elif type(o).__name__ == 'DataFrame':  # avoid pandas import
            o.columns = o.columns.droplevel('channel')  # flatten MultiIndex
            return o.to_dict(orient='index')

        elif type(o) is type and o in data_types:
            return data_types[o]

        return json.JSONEncoder.default(self, o)


def to_json(obj):
    return json.dumps(obj, cls=Encoder, indent=2, ignore_nan=True)


DR8North = None
DR8South = None


def init_db():
    url = 'postgresql://{}:{}@{}:{}/{}'

    # this creates the connection to the main database

    user = get_secret('username')
    password = get_secret('password')
    host = get_secret('host')
    port = get_secret('port')
    database = get_secret('dbname')

    fmturl = url.format(user, password or '', host or '', port or '', database)
    mainconn = sa.create_engine(fmturl, client_encoding='utf8')

    DBSession.configure(bind=mainconn)
    Base.metadata.bind = mainconn

    return mainconn


def url_db():
    url = 'postgresql://{}:{}@{}:{}/{}'

    # this creates the connection to the main database

    user = get_secret('username')
    password = get_secret('password')
    host = get_secret('host')
    port = get_secret('port')
    database = get_secret('dbname')

    fmturl = url.format(user, password or '', host or '', port or '', database)

    return fmturl


class BaseMixin(object):
    query = DBSession.query_property()
    id = sa.Column(sa.Integer, primary_key=True)

    created_at = sa.Column(sa.DateTime, nullable=False,
                           server_default=sa.func.now())
    modified = sa.Column(sa.DateTime, nullable=False,
                         server_default=sa.func.now(),
                         server_onupdate=sa.func.now())

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower() + 's'

    __mapper_args__ = {'confirm_deleted_rows': False}

    def __str__(self):
        return to_json(self)

    def __repr__(self):
        attr_list = [f"{c.name}={getattr(self, c.name)}"
                     for c in self.__table__.columns]
        return f"<{type(self).__name__}({', '.join(attr_list)})>"

    def to_dict(self):
        if sa.inspection.inspect(self).expired:
            DBSession().refresh(self)
        return {k: v for k, v in self.__dict__.items()
                if not k.startswith('_')}

    @classmethod
    def create_or_get(cls, id):
        obj = cls.query.get(id)
        if obj is not None:
            return obj
        else:
            return cls(id=id)


Base = declarative_base(cls=BaseMixin)


def join_model(join_table, model_1, model_2, column_1=None, column_2=None,
               fk_1='id', fk_2='id', base=Base):
    """Helper function to create a join table for a many-to-many relationship.
    Parameters
    ----------
    join_table : str
        Name of the new table to be created.
    model_1 : str
        First model in the relationship.
    model_2 : str
        Second model in the relationship.
    column_1 : str, optional
        Name of the join table column corresponding to `model_1`. If `None`,
        then {`table1`[:-1]_id} will be used (e.g., `user_id` for `users`).
    column_2 : str, optional
        Name of the join table column corresponding to `model_2`. If `None`,
        then {`table2`[:-1]_id} will be used (e.g., `user_id` for `users`).
    fk_1 : str, optional
        Name of the column from `model_1` that the foreign key should refer to.
    fk_2 : str, optional
        Name of the column from `model_2` that the foreign key should refer to.
    base : sqlalchemy.ext.declarative.api.DeclarativeMeta
        SQLAlchemy model base to subclass.
    Returns
    -------
    sqlalchemy.ext.declarative.api.DeclarativeMeta
        SQLAlchemy association model class
    """
    table_1 = model_1.__tablename__
    table_2 = model_2.__tablename__
    if column_1 is None:
        column_1 = f'{table_1[:-1]}_id'
    if column_2 is None:
        column_2 = f'{table_2[:-1]}_id'

    model_attrs = {
        '__tablename__': join_table,
        'id': None,
        column_1: sa.Column(column_1, sa.ForeignKey(f'{table_1}.{fk_1}',
                                                    ondelete='CASCADE'),
                            primary_key=True),
        column_2: sa.Column(column_2, sa.ForeignKey(f'{table_2}.{fk_2}',
                                                    ondelete='CASCADE'),
                            primary_key=True),
        model_1.__name__.lower(): relationship(model_1, cascade='all'),
        model_2.__name__.lower(): relationship(model_2, cascade='all')
    }
    model = type(model_1.__name__ + model_2.__name__, (base,), model_attrs)

    return model


class SpatiallyIndexed(object):

    ra = sa.Column(psql.DOUBLE_PRECISION)
    dec = sa.Column(psql.DOUBLE_PRECISION)

    @declared_attr
    def __table_args__(cls):
        tn = cls.__tablename__
        return sa.Index(f'{tn}_q3c_ang2ipix_idx',
                        sa.func.q3c_ang2ipix(cls.ra, cls.dec)),


class HasHeader(object):

    header = sa.Column(psql.JSONB)

    @classmethod
    def from_file(cls, f):
        obj = cls()
        with fits.open(f) as hdul:
            hd = dict(hdul[0].header)
        hd2 = hd.copy()
        for k in hd:
            if not isinstance(hd[k], (int, str, bool, float)):
                del hd2[k]
        obj.header = hd2

        return obj

    @property
    def astropy_header(self):
        header = fits.Header()
        header.update(self.header)
        return header


class Exposure(SpatiallyIndexed, HasHeader, Base):

    filename = sa.Column(sa.Text, nullable=False, comment='Name of this file')
    mjd = sa.Column(psql.DOUBLE_PRECISION, comment='MJD of observation start')
    event_name = sa.Column(sa.Text, comment='Object name')
    filter = sa.Column(sa.Text)
    graceid = sa.Column(sa.Text)

    images = relationship('Image', cascade='all')
    subtractions = relationship('Subtraction', cascade='all')

    @classmethod
    def from_fitsfz(cls, path):

        self = super(Exposure, cls).from_file(path)
        with fits.open(path) as hdul:
            hdr = hdul[0].header

        coord = SkyCoord(hdr['RA'], hdr['DEC'], unit=['hourangle', 'degree'])
        self.ra = coord.ra.deg
        self.dec = coord.dec.deg
        self.filename = os.path.basename(path)
        self.mjd = hdr['MJD-OBS']
        self.filter = hdr['FILTER'][0]
        self.graceid = os.getenv('S3_TRIGGER_NAME')

        return self

    def submit_to_treasuremap(self):

        BASE = 'http://treasuremap.space/api/v0/'
        TARGET = 'pointings'

        json_data = {
            "graceid": self.graceid,
            "api_token": get_secret('TREASUREMAP_API_TOKEN'),
            "pointings": [{
                "ra": self.ra,
                "dec": self.dec,
                "band": self.filter,
                "instrumentid": "38",  # decam
                "time": self.header['DATE-OBS'].astext.cast(sa.DateTime),
                "status": "planned"
            }]
        }

        r = requests.post(url=BASE+TARGET, json=json_data)
        print(r.text)


class ImageMixin(HasHeader, SpatiallyIndexed):

    # corners
    ra1 = sa.Column(psql.DOUBLE_PRECISION)
    dec1 = sa.Column(psql.DOUBLE_PRECISION)
    ra2 = sa.Column(psql.DOUBLE_PRECISION)
    dec2 = sa.Column(psql.DOUBLE_PRECISION)
    ra3 = sa.Column(psql.DOUBLE_PRECISION)
    dec3 = sa.Column(psql.DOUBLE_PRECISION)
    ra4 = sa.Column(psql.DOUBLE_PRECISION)
    dec4 = sa.Column(psql.DOUBLE_PRECISION)

    # other useful info
    lmt_mg = sa.Column(sa.Float)
    seeing = sa.Column(sa.Float, comment='FWHM of seeing disc in arcsec')
    skysig = sa.Column(sa.Float)
    magzp = sa.Column(sa.Float)
    ccdnum = sa.Column(sa.Integer)
    basename = sa.Column(sa.Text)

    @hybrid_property
    def filter(self):
        return self.exposure.filter

    @hybrid_property
    def mjd(self):
        return self.exposure.mjd

    @hybrid_property
    def gain(self):
        return 0.5 * (self.header['GAINA'] + self.header['GAINB'])

    @gain.expression
    def gain(self):
        return 0.5 * (sa.cast(self.header['GAINA'], sa.Float) +
                      sa.cast(self.header['GAINB'], sa.Float))

    @classmethod
    def from_file(cls, path):
        self = super(ImageMixin, cls).from_file(path)
        with fits.open(path) as hdul:
            hdr = hdul[0].header

        naxis1 = hdr['NAXIS1']
        naxis2 = hdr['NAXIS2']
        wcs = WCS(hdr)
        footprint = wcs.calc_footprint()

        for i, row in enumerate(footprint):
            setattr(self, f'ra{i+1}', float(row[0]))
            setattr(self, f'dec{i+1}', float(row[1]))

        center = wcs.all_pix2world([[naxis1 / 2, naxis2 / 2]], 1)[0]
        self.ra = float(center[0])
        self.dec = float(center[1])
        self.seeing = hdr['SEEING']
        self.magzp = hdr['MAGZP']
        self.skysig = hdr['SKYSIG']
        self.lmt_mg = hdr['LMT_MG']
        self.exposure_id = hdr['EXPID']
        self.ccdnum = hdr['CCDNUM']
        self.basename = os.path.basename(path)

        return self

    @hybrid_property
    def poly(self):
        return array([self.ra1, self.dec1, self.ra2, self.dec2,
                      self.ra3, self.dec3, self.ra4, self.dec4])

    @declared_attr
    def exposure_id(cls):
        return sa.Column(sa.Integer,
                         sa.ForeignKey('exposures.id', ondelete='CASCADE'),
                         nullable=False)

    @declared_attr
    def exposure(cls):
        tn = cls.__tablename__
        return relationship('Exposure', back_populates=tn, cascade='all')

    @declared_attr
    def objects(cls):
        return relationship('Object', cascade='all')

    @property
    def s3_location(self):
        return f'http://decam-growth-ligo.s3.amazonaws.com/' \
               f'{self.exposure.graceid}/' \
               f'{self.exposure.filename.split(".")[0]}/{self.basename}'


class Image(ImageMixin, Base):
    subtraction = relationship('Subtraction', cascade='all', uselist=False)
    objects = relationship('Object', cascade='all', back_populates='image',
                           secondary='subtractions')

    detection_limit = sa.Column(sa.Float)


class Subtraction(ImageMixin, Base):
    image_id = sa.Column(sa.Integer,
                         sa.ForeignKey('images.id', ondelete='CASCADE'),
                         index=True)
    image = relationship('Image', cascade='all')
    candidates_detected = relationship('Candidate',
                                       cascade='all', secondary='objects',
                                       back_populates='subtractions_detected')

    forced_photometry = relationship('ForcedPhotometry', cascade='all')
    jpegs = relationship('JPEG', cascade='all')
    template_header = sa.Column(psql.JSONB)


class Object(SpatiallyIndexed, Base):

    candidate_id = sa.Column(sa.Text,
                             sa.ForeignKey('candidates.id',
                                           ondelete='SET NULL'),
                             nullable=True)
    candidate = relationship('Candidate',
                             back_populates='objects', cascade='all',
                             uselist=False)

    subtraction_id = sa.Column(sa.Integer,
                               sa.ForeignKey('subtractions.id',
                                             ondelete='CASCADE'),
                               nullable=False)
    subtraction = relationship('Subtraction',
                               back_populates='objects', cascade='all',
                               uselist=False)

    image = relationship('Image',
                         back_populates='objects',
                         cascade='all', secondary='subtractions',
                         uselist=False)

    rb = sa.Column(sa.Float)
    name = sa.Column(sa.Text)

    mag = sa.Column(sa.Float)
    magerr = sa.Column(sa.Float)

    flux = sa.Column(sa.Float)
    fluxerr = sa.Column(sa.Float)

    @hybrid_property
    def mjd(self):
        return self.subtraction.mjd

    @hybrid_property
    def maglim(self):
        return self.subtraction.lmt_mg

    @hybrid_property
    def filter(self):
        return self.subtraction.filter

    @hybrid_property
    def ccdnum(self):
        return self.subtraction.ccdnum

    @hybrid_property
    def magzp(self):
        return self.subtraction.magzp

    ignore = sa.Column(sa.Boolean, server_default='f')
    cutouts = relationship('Cutout', cascade='all')


class ForcedPhotometry(Base):

    __tablename__ = 'forcedphotometry'

    candidate_id = sa.Column(sa.Text,
                             sa.ForeignKey('candidates.id',
                                           ondelete='SET NULL'),
                             nullable=True,
                             index=True)
    candidate = relationship('Candidate',
                             back_populates='forced_photometry',
                             cascade='all', uselist=False)

    subtraction_id = sa.Column(sa.Integer,
                               sa.ForeignKey('subtractions.id',
                                             ondelete='CASCADE'),
                               nullable=False,
                               index=True)
    subtraction = relationship('Subtraction', cascade='all', uselist=False)

    flux = sa.Column(sa.Float)
    fluxerr = sa.Column(sa.Float)
    status = sa.Column(sa.Boolean)
    reason = sa.Column(sa.Text)

    def pull_cutout_and_show(self, img_type='sub'):

        if not self.status:
            return

        if img_type == 'sub':
            s3_location = self.subtraction.s3_location
            basename = self.subtraction.basename
        elif img_type == 'new':
            s3_location = self.subtraction.image.s3_location
            basename = self.subtraction.image.basename
        elif img_type == 'ref':
            s3_location = self.subtraction.image.s3_location
            s3_location = s3_location.replace('.fits', '.template.fits')
            basename = self.subtraction.image.basename.replace('.fits',
                                                               '.template.fits'
                                                               )

        if not os.path.exists(basename):

            r = requests.get(s3_location)
            r.raise_for_status()
            with open(basename, 'wb') as f:
                f.write(r.content)

            print(f'Downloaded "{s3_location}"')

        regbase = self.subtraction.basename.replace('.fits', '.reg')

        if not os.path.exists(regbase):
            r = requests.get(self.subtraction.s3_location.replace('.fits',
                                                                  '.reg'))
            r.raise_for_status()
            with open(regbase, 'wb') as f:
                f.write(r.content)

            print(f'Downloaded "{regbase}"')

        psf = self.subtraction.image.s3_location.replace('.fits', '.psf')
        pbase = self.subtraction.image.basename.replace('.fits', '.psf')
        if not os.path.exists(pbase):
            r = requests.get(psf)
            r.raise_for_status()

            with open(pbase, 'wb') as f:
                f.write(r.content)
            print(f'Downloaded "{pbase}"')

        cmd = f'ds9 -zscale {basename} -region {regbase} ' \
              f'-lock frame wcs -pan ' \
              f'to {self.candidate.ra} {self.candidate.dec} wcs fk5'
        print(cmd)

        return basename, pbase

    @hybrid_property
    def adjusted_fluxerr(self):
        return self.adjusted_magerr / 1.0826 * self.flux

    @hybrid_property
    def mjd(self):
        return self.subtraction.mjd

    @hybrid_method
    def maglim(self, nsigma):
        return -2.5 * np.log10(self.fluxerr * nsigma) + self.magzp

    @maglim.expression
    def maglim(self, nsigma):
        return -2.5 * sa.func.log(self.fluxerr * nsigma) + self.magzp

    @hybrid_property
    def filter(self):
        return self.subtraction.filter

    @hybrid_property
    def ccdnum(self):
        return self.subtraction.ccdnum

    @hybrid_property
    def magzp(self):
        return self.subtraction.magzp

    @hybrid_property
    def mag(self):
        return -2.5 * np.log10(self.flux) + self.magzp

    @mag.expression
    def mag(self):
        return -2.5 * sa.func.log(self.flux) + self.magzp

    @hybrid_property
    def magerr(self):
        return 1.0826 * self.fluxerr / self.flux

    @property
    def image(self):
        return self.subtraction.image

    @hybrid_property
    def snr(self):
        return self.flux / self.fluxerr

    @hybrid_property
    def adjusted_magerr(self):
        return np.sqrt(self.magerr**2 +
                       self.subtraction.header['MAGZPERR']**2 +
                       self.subtraction.template_header['MAGZPERR']**2)

    @adjusted_magerr.expression
    def adjusted_magerr(self):
        return sa.func.sqrt(self.magerr**2 +
                            self.subtraction.header['MAGZPERR']**2 +
                            self.subtraction.template_header['MAGZPERR']**2)


namenum_seq = sa.Sequence('namenum')


class Candidate(SpatiallyIndexed, Base):

    id = sa.Column(sa.Text, primary_key=True)

    # TODO deprecate these columns
    nmatches = sa.Column(sa.Integer)
    sg_gaia = sa.Column(sa.Integer)

    oldname = sa.Column(sa.Text)

    @property
    def subtractions_containing_coordinate(self):
        return DBSession().query(Subtraction).filter(
            sa.func.q3c_radial_query(Subtraction.ra, Subtraction.dec,
                                     self.ra, self.dec, 0.182),
            sa.func.q3c_poly_query(self.ra, self.dec, Subtraction.poly)
        ).all()

    @property
    def images_containing_coordinate(self):
        return DBSession().query(Image).filter(
            sa.func.q3c_radial_query(Image.ra, Image.dec,
                                     self.ra, self.dec, 0.182),
            sa.func.q3c_poly_query(self.ra, self.dec, Image.poly)
        ).all()

    objects = relationship('Object', cascade='all')
    subtractions_detected = relationship('Subtraction',
                                         cascade='all', secondary='objects',
                                         back_populates='candidates_detected')

    clura = sa.Column(psql.DOUBLE_PRECISION)
    cludec = sa.Column(psql.DOUBLE_PRECISION)
    cludistmpc = sa.Column(psql.DOUBLE_PRECISION)
    cluname = sa.Column(sa.Text)
    clumstar = sa.Column(sa.Float)
    clusfr_fuv = sa.Column(sa.Float)
    clusfr_ha = sa.Column(sa.Float)
    cluid = sa.Column(sa.Integer)
    cluseparcsec = sa.Column(sa.Float)
    clusepkpc = sa.Column(sa.Float)
    parallax = sa.Column(sa.Float)

    # legacy survey phot-z's
    z_phot_med = sa.Column(sa.Float)
    z_phot_avg = sa.Column(sa.Float)
    z_phot_l68 = sa.Column(sa.Float)
    z_phot_u68 = sa.Column(sa.Float)
    z_phot_l95 = sa.Column(sa.Float)
    z_phot_u95 = sa.Column(sa.Float)
    dr8_id = sa.Column(sa.Integer)
    dr8_type = sa.Column(sa.Text)
    dr8_dist = sa.Column(sa.Float)

    forced_photometry = relationship('ForcedPhotometry', cascade='all')
    jpegs = relationship('JPEG', cascade='all')

    @property
    def light_curve(self):
        lcraw = []
        columns = ['mjd', 'filter', 'flux', 'fluxerr',
                   'mag', 'magerr', 'maglimit', 'zp', 'zpsys', 'fpid']
        for fp in self.forced_photometry:
            if fp.status:
                lcraw.append([
                    fp.mjd,
                    fp.filter,
                    fp.flux,
                    fp.adjusted_fluxerr,
                    fp.mag if fp.snr >= 5 else -99.,
                    fp.adjusted_magerr if fp.snr >= 5 else -99.,
                    fp.maglim(5),
                    fp.magzp,
                    'ab',
                    fp.id
                ])
        lc = Table.from_pandas(pd.DataFrame(lcraw, columns=columns))
        lc.sort('mjd')
        return lc

    def fit_lc(self, band):
        from scipy.optimize import minimize
        from sncosmo.photdata import PhotometricData

        lc = self.light_curve
        # keep only detections in this filter
        lc = lc[lc['filter'] == band]

        # define an objective function for the fit

        clc = lc.copy()
        clc['filter'] = ['des' + f for f in clc['filter']]

        data = PhotometricData(clc)
        data = data.normalized()

        def objective_function(parameters):
            m, t0, b = parameters
            fluxes = m * (data.time - t0) + b
            chisq = np.sum((fluxes - data.flux)**2 / data.fluxerr**2)
            return chisq

        x0 = (0., data.time.mean(), data.flux.mean())
        minres = minimize(objective_function, x0, method='L-BFGS-B')

        if not minres['success']:
            raise RuntimeError(minres['message'])

        return minres.x

    def pull_cutouts_and_show(self, img_type='sub', save=False):

        cmd = 'ds9 -zscale '
        images = []
        regions = []

        if save:
            import matplotlib.pyplot as plt
            from matplotlib.patches import Circle
            from astropy.visualization import ZScaleInterval
            from astropy.nddata.utils import Cutout2D
            ncutouts = len(self.forced_photometry)
            ncols = 8
            nrows = ncutouts // ncols + 1
            fig, ax = plt.subplots(figsize=(8.5, 11), nrows=nrows, ncols=ncols)

        for fp in self.forced_photometry:
            if not fp.status:
                continue

            if img_type == 'sub':
                s3_location = fp.subtraction.s3_location
                basename = fp.subtraction.basename
            elif img_type == 'new':
                s3_location = fp.subtraction.image.s3_location
                basename = fp.subtraction.image.basename
            elif img_type == 'ref':
                s3_location = fp.subtraction.image.s3_location
                s3_location = s3_location.replace('.fits', '.template.fits')
                basename = fp.subtraction.image.basename
                basename = basename.replace('.fits', '.template.fits')

            if not os.path.exists(basename):

                r = requests.get(s3_location)
                try:
                    r.raise_for_status()
                except requests.HTTPError:
                    print(f"Unable to download {s3_location}")
                with open(basename, 'wb') as f:
                    f.write(r.content)

                print(f'Downloaded "{s3_location}"')

            images.append(basename)

            regbase = fp.subtraction.basename.replace('.fits', '.reg')

            if not os.path.exists(regbase):
                fn = fp.subtraction.s3_location.replace('.fits', '.reg')
                r = requests.get(fn)
                try:
                    r.raise_for_status()
                except requests.HTTPError:
                    fn = fp.subtraction.s3_location.replace('.fits', '.reg')
                    print(f"Unable to download {fn}")
                    continue
                with open(regbase, 'wb') as f:
                    f.write(r.content)

                print(f'Downloaded "{regbase}"')

            regions.append(regbase)

        for image, region in zip(images, regions):
            cmd += f'{image} -region {region} '

        cmd += f'-lock frame wcs -pan to {self.ra} {self.dec} wcs fk5'
        print(cmd)

        if save:
            for fp, a in zip(sorted(self.forced_photometry,
                                    key=lambda f: f.mjd), ax.ravel()):

                if img_type == 'sub':
                    basename = fp.subtraction.basename
                elif img_type == 'new':
                    basename = fp.subtraction.image.basename
                elif img_type == 'ref':
                    basename = fp.subtraction.image.basename
                    basename = basename.replace('.fits', '.template.fits')

                try:
                    hdul = fits.open(basename)
                except FileNotFoundError:
                    continue

                data = hdul[0].data
                wcs = WCS(hdul[0].header)
                radec = SkyCoord(self.ra, self.dec, unit='deg')
                cutout = Cutout2D(data, radec, size=51, wcs=wcs,
                                  mode='partial')
                interval = ZScaleInterval()
                zmin, zmax = interval.get_limits(data)
                data = cutout.data
                a.imshow(data, vmin=zmin, vmax=zmax, cmap='gray')
                a.set_title(f'{fp.mjd:0.5f},{fp.filter}', fontsize=6)
                a.set_xticks([])
                a.set_yticks([])
                try:
                    f = open(fp.subtraction.basename.replace('.fits', '.reg'),
                             'r')
                except FileNotFoundError:
                    continue

                xyc = []

                for line in f:
                    if line.startswith('circle'):
                        lspl = line.split(')')[0].split('(')[1].split(',')[:2]
                        x, y = list(map(float, lspl))
                        color = line.split('color=')[1]
                        xyc.append([x, y, color.rstrip()])

                table = Table.from_pandas(pd.DataFrame(xyc,
                                                       columns=['x',
                                                                'y',
                                                                'color']))
                myx, myy = wcs.all_world2pix([[self.ra, self.dec]], 1)[0]

                for row in table:
                    if np.sqrt((row['x'] - myx)**2 + (row['y'] - myy)**2) < 4:
                        c = Circle((25, 25), 10)
                        c.set_facecolor('none')
                        c.set_edgecolor(row['color'])
                        a.add_artist(c)
                hdul.close()

            for a in ax.ravel()[len(self.forced_photometry):]:
                a.set_visible(False)

            fig.tight_layout()
            os.remove(basename)
            os.remove(basename.replace('.fits', '.reg'))
            fig.savefig(f'{self.oldname}.{self.id}.cutouts.{img_type}.pdf',
                        bbox_inches='tight')

    def plot_lc(self, fit=False, flux=False):
        from matplotlib import pyplot as plt
        lc = self.light_curve
        fig, ax = plt.subplots()
        colors = {'g': 'g', 'r': 'r', 'i': 'orange', 'z': 'k'}

        if not flux:

            for lcg in lc.group_by('filter').groups:
                filt = lcg[0]['filter']
                color = colors[filt]
                ok = lcg['mag'] != -99
                oklc = lcg[ok]
                ax.errorbar(oklc['mjd'], oklc['mag'], yerr=oklc['magerr'],
                            marker='.', markersize=8,
                            linestyle='none', color=color, label=filt)
                limlc = lcg[~ok]
                ax.scatter(limlc['mjd'], limlc['maglimit'],
                           marker='v', color=color)

                if fit:
                    m, t0, b = self.fit_lc(band=filt)
                    t = np.linspace(lcg['mjd'].min(), lcg['mjd'].max())
                    ax.plot(t, -2.5 * np.log10(m * (t - t0) + b) + 25.,
                            color=color)
            ax.invert_yaxis()

        else:

            from sncosmo.photdata import PhotometricData
            for lcg in lc.group_by('filter').groups:
                filt = lcg[0]['filter']
                color = colors[filt]

                clc = lcg.copy()
                clc['filter'] = ['des' + f for f in clc['filter']]
                data = PhotometricData(clc)
                data = data.normalized(zp=25, zpsys='ab')

                ax.errorbar(data.time, data.flux,
                            yerr=data.fluxerr, marker='.', markersize=8,
                            linestyle='none', color=color, label=filt)

                if fit:
                    m, t0, b = self.fit_lc(band=filt)
                    t = np.linspace(lcg['mjd'].min(), lcg['mjd'].max())
                    ax.plot(t, m * (t - t0) + b, color=color)

        ax.legend()

        return fig

    def dmag_dt(self, band):

        m, t0, b = self.fit_lc(band)

        lc = self.light_curve
        lc = lc[lc['filter'] == band]
        t = np.linspace(lc['mjd'].min(), lc['mjd'].max(), 50)

        mag = -2.5 * np.log10(m * (t - t0) + b) - 25.
        dmdt = np.gradient(mag) / np.gradient(t)
        return t, dmdt


def candidates_contained(cls):
    return DBSession().query(Candidate).filter(
        sa.func.q3c_poly_query(Candidate.ra, Candidate.dec, cls.poly)
    ).all()


ImageMixin.candidates_contained = property(candidates_contained)


class Cutout(Base):

    # this is what is displayed on the marshal

    object_id = sa.Column(sa.Integer,
                          sa.ForeignKey('objects.id',
                                        ondelete='CASCADE'),
                          index=True)
    object = relationship('Object', cascade='all')

    name = sa.Column(sa.Text)
    sci_image = sa.Column(psql.BYTEA)
    ref_image = sa.Column(psql.BYTEA)
    diff_image = sa.Column(psql.BYTEA)

    @classmethod
    def from_dict(cls, dict, object):
        obj = cls()
        obj.object = object
        obj.ref_image = to_bytes(dict['ref'])
        obj.diff_image = to_bytes(dict['sub'])
        obj.sci_image = to_bytes(dict['new'])
        return obj


class NumpyArray(sa.types.TypeDecorator):
    impl = psql.ARRAY(psql.REAL)

    def process_result_value(self, value, dialect):
        return np.array(value)


class JPEG(Base):

    candidate_id = sa.Column(sa.Text,
                             sa.ForeignKey('candidates.id',
                                           ondelete='CASCADE'), index=True)
    candidate = relationship('Candidate',
                             back_populates='jpegs', cascade='all')

    subtraction_id = sa.Column(sa.Integer,
                               sa.ForeignKey('subtractions.id',
                                             ondelete='CASCADE'),
                               index=True)
    subtraction = relationship('Subtraction',
                               back_populates='jpegs', cascade='all')

    zmin = sa.Column(sa.Float)
    zmax = sa.Column(sa.Float)

    # this is what is displayed on the examine page
    data = sa.Column(NumpyArray)
    cutout_type = sa.Column(sa.Text)

    @property
    def raw_jpg(self):

        return to_bytes(self.data)
