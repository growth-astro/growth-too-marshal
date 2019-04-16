import sys
import functools
import warnings
import datetime
from datetime import timedelta
import os
import urllib.parse
import math
from io import StringIO
import re
import requests
import tempfile
import pkg_resources

from celery import group
import numpy as np
from scipy.stats import norm
from astropy import time
import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.table import Table
from astropy_healpix import HEALPix
import pandas as pd
import h5py
import healpy as hp
from ligo.skymap import io
from ligo.skymap.tool.ligo_skymap_plot_airmass import main as plot_airmass
import matplotlib.pyplot as plt
import matplotlib.style

from flask import (
    abort, flash, jsonify, make_response, redirect, render_template, request,
    Response, url_for)
from flask_login import (
    current_user, login_required, login_user, logout_user, LoginManager)
from wtforms import (
    BooleanField, FloatField, Form, RadioField, SubmitField, TextField)
from wtforms_components.fields import (
    DateTimeField, DecimalSliderField, SelectField)
from wtforms import validators
from passlib.apache import HtpasswdFile
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from pyvo.dal import TAPService

from .flask import app
from .jinja import atob
from . import models, tasks

#
#
# From http://wtforms-alchemy.readthedocs.io/en/latest/advanced.html#using-wtforms-alchemy-with-flask-wtf  # noqa: E501
from flask_wtf import FlaskForm
from wtforms_alchemy import model_form_factory
# The variable db here is a SQLAlchemy object instance from
# Flask-SQLAlchemy package

BaseModelForm = model_form_factory(FlaskForm)


class ModelForm(BaseModelForm):
    @classmethod
    def get_session(cls):
        return models.db.session
#
#
#


try:
    htpasswd = HtpasswdFile(os.path.join(app.instance_path, 'htpasswd'))
except FileNotFoundError:
    htpasswd = HtpasswdFile()
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'


def cached_as_if_static(func):
    """Specify that a view should be cached as if it was a static file."""
    @functools.wraps(func)
    def decorated_function(*args, **kwargs):
        response = make_response(func(*args, **kwargs))
        response.cache_control.max_age = app.get_send_file_max_age(None)
        return response
    return decorated_function


def one_or_404(query):
    # FIXME: https://github.com/mitsuhiko/flask-sqlalchemy/pull/527
    rv = query.one_or_none()
    if rv is None:
        abort(404)
    else:
        return rv


@login_manager.user_loader
def load_user(user_id):
    # FIXME: new users will have entries in the htpasswd file but not in
    # the database. Once the htpasswd file goes away, drop everything after
    # the `or`.
    return models.User.query.get(user_id) or models.User(name=user_id)


def get_marshallink(dateobs):

    try:
        from . import growthdb
    except OperationalError:
        warnings.warn('growth-db does not appear to be accessible.')
        return 'None'

        event = models.Event.query.filter_by(dateobs=dateobs).all()
        if len(event) == 0 or event is None:
            marshallink = 'None'
        else:
            event = event[0]
            scienceprogram = None
            if 'Fermi' in event.tags:
                scienceprogram = 'GBM'
            elif 'GW' in event.tags:
                scienceprogram = \
                    'Electromagnetic Counterparts to Gravitational Waves'
            elif 'AMON' in event.tags:
                scienceprogram = 'IceCube'
        return growthdb.get_marshallink(current_user.name, scienceprogram)


def human_time(*args, **kwargs):
    secs = float(datetime.timedelta(*args, **kwargs).total_seconds())
    units = [("day", 86400), ("hour", 3600), ("minute", 60), ("second", 1)]
    parts = []
    for unit, mul in units:
        if secs / mul >= 1 or mul == 1:
            if mul > 1:
                n = int(math.floor(secs / mul))
                secs -= n * mul
            else:
                n = secs if secs != int(secs) else int(secs)
            parts.append("%s %s%s" % (n, unit, "" if n == 1 else "s"))
    return parts[0]


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if htpasswd.check_password(request.form['user'],
                                   request.form['password']):
            login_user(load_user(request.form['user']),
                       remember=('remember' in request.form))
            flash('You are now logged in.', 'success')

            next = request.args.get('next')
            return redirect(next or url_for('index'))
        else:
            flash('Invalid username or password.', 'danger')
    return render_template('login.html')


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))


@app.route('/')
@login_required
def index():
    return render_template(
        'index.html',
        events=models.Event.query.order_by(
            models.db.desc(models.Event.dateobs)))


class DeleteForm(ModelForm):

    queue_name = SelectField()


@app.route('/queue/', methods=['GET', 'POST'])
@login_required
def queue():

    r = requests.get(urllib.parse.urljoin(tasks.scheduler.ZTF_URL,
                                          'queues'), json={})
    data_all = r.json()
    queue_names_list = []
    for data in data_all:
        queue_names_list.append(data['queue_name'])
    queue_names = "\n".join(queue_names_list)

    r = requests.get(
        urllib.parse.urljoin(tasks.scheduler.ZTF_URL, 'current_queue'))
    data = r.json()
    queue_info = []
    queue_info.append('Current queue information:')
    queue_info.append(f"   Queue name: {data['queue_name']}")
    queue_info.append(f"   Queue type: {data['queue_type']}")
    queue = pd.read_json(data['queue'], orient='records')
    queue_info.append(f"   Number of queued requests: {len(queue)}")
    if len(queue) > 0:
        n_fields = len(queue['field_id'].unique())
        queue_info.append(f"   Number of unique field_ids: {n_fields}")
        w = queue['ordered']
        if np.sum(w) > 0:
            queue_info.append(f"   Ordered requests:")
            queue_info.append(queue.loc[w, ['field_id', 'ra', 'dec',
                                            'filter_id', 'program_id',
                                            'subprogram_name']].to_string())
        queue_info.append(f"   Unordered requests:")
        if 'slot_start_time' in queue.columns:
            grp = queue[~w].groupby('slot_start_time')
            for start_time, rows in grp:
                queue_info.append(f"      {start_time}:")

                rowstr = rows.to_csv(header=False,
                                     columns=('field_id', 'ra', 'dec',
                                              'filter_id', 'program_id',
                                              'subprogram_name'))
                queue_info = queue_info + rowstr.split("\n")
            pass
        else:
            queue_info.append(queue.loc[~w, ['field_id', 'ra', 'dec',
                                             'filter_id', 'program_id',
                                             'subprogram_name']].to_string())
    queue_info = "\n".join(queue_info)

    form = DeleteForm(request.form)
    form.queue_name.choices = [
        (queue_name,) * 2 for queue_name in queue_names_list]

    if request.method == 'POST' and form.validate():
        queue_name = form.queue_name.data
        r = requests.delete(
            urllib.parse.urljoin(tasks.scheduler.ZTF_URL, 'queues'),
            json={'queue_name': queue_name})

        flash('Deleted observing plan "{}".'.format(queue_name),
              'success')
        return redirect(url_for('queue'))

    return render_template(
        'queue.html',
        form=form,
        queue_names=queue_names,
        queue_info=queue_info)


@app.route('/event/<datetime:dateobs>')
@login_required
def event(dateobs):
    if isinstance(dateobs, datetime.date) \
            and not isinstance(dateobs, datetime.datetime):
        query = models.Event.query.filter(
            models.db.cast(models.Event.dateobs, models.db.Date) == dateobs) \
            .order_by(models.db.desc(models.Event.dateobs))
        try:
            event = query.one()
        except NoResultFound:
            abort(404)
        except MultipleResultsFound:
            return make_response(render_template('multiple_events.html',
                                                 date=dateobs,
                                                 events=query), 300)
        return redirect(url_for('event', dateobs=event.dateobs))

    return render_template(
        'event.html', event=models.Event.query.get_or_404(dateobs),
        marshallink=get_marshallink(dateobs))


@app.route('/event/<datetime:dateobs>/objects')
@login_required
def objects(dateobs):
    return render_template(
        'objects.html', event=models.Event.query.get_or_404(dateobs),
        marshallink=get_marshallink(dateobs))


@app.route('/event/<datetime:dateobs>/plan', methods=['GET', 'POST'])
@login_required
def plan(dateobs):
    if request.method == 'POST':
        form = dict(request.form)

        # Determine which form button was clicked.
        for command in 'delete', 'go':
            if form.pop(command, None) is not None:
                break
        else:
            # Should not be reached.
            raise ValueError(
                'Invalid form data: '
                'could not determine which button was selected.')

        # Determine which plans were selected.
        plans = [
            [atob(key_part) for key_part in key.split('_')]
            for key in form]
        if not plans:
            raise ValueError('Invalid form data: no plans were selected.')

        if command == 'delete':
            for telescope, plan_name in plans:
                models.Plan.query.filter_by(
                    dateobs=dateobs, telescope=telescope, plan_name=plan_name
                ).delete()
            models.db.session.commit()
            flash('Deleted plans.', 'success')

        if command == 'go':
            group(
                group(
                    tasks.scheduler.submit.s(telescope, plan_name),
                    tasks.email.compose_too.s(telescope, plan_name)
                )
                for telescope, plan_name in plans
            ).delay(dateobs)
            flash('Submitted plans to queue.', 'success')

    return render_template(
        'plan.html', event=models.Event.query.get_or_404(dateobs))


@app.route('/event/<datetime:dateobs>/plan/download/telescope/<telescope>/<plan_name>.json')
def download_json(dateobs, telescope, plan_name):

    plan = models.Plan.query.filter_by(
        dateobs=dateobs, telescope=telescope, plan_name=plan_name).one()
    json_data, queue_name = get_json_data(plan)

    return jsonify(json_data)


@app.route('/event/<datetime:dateobs>/plan/telescope/<telescope>/<plan_name>/json')
def plan_json(dateobs, telescope, plan_name):
    return jsonify([
        planned_observation.field_id
        for planned_observation in
        one_or_404(models.Plan.query.filter_by(dateobs=dateobs, telescope=telescope, plan_name=plan_name)).planned_observations
    ])


class PlanForm(ModelForm):

    class Meta:
        model = models.Plan
        exclude = ['plan_args']

    dateobs = DateTimeField()

    telescope = SelectField(default='ZTF')

    localization = SelectField()

    def _localization_query(self):
        return models.Localization.query.filter_by(dateobs=self.dateobs.data)

    validity_window_start = DateTimeField(
        format='%Y-%m-%d %H:%M:%S',
        default=lambda: datetime.datetime.utcnow(),
        validators=[validators.DataRequired()])

    validity_window_end = DateTimeField(
        format='%Y-%m-%d %H:%M:%S',
        default=lambda: datetime.datetime.utcnow() + datetime.timedelta(1),
        validators=[validators.DataRequired()])

    filters = TextField('filters', validators=[validators.DataRequired()])

    schedule = RadioField(
        choices=[('greedy', 'greedy'), ('sear', 'sear'),
                 ('airmass_weighted', 'airmass_weighted')],
        default='greedy')

    dither = BooleanField(default=False)

    references = BooleanField(default=False)

    filterschedule = RadioField(
        choices=[('block', 'block'), ('integrated', 'integrated')],
        default='block')

    schedule_strategy = RadioField(
        choices=[('tiling', 'tiling'), ('catalog', 'catalog')],
        default='tiling')

    exposure_time = FloatField(
        default=300,
        validators=[validators.DataRequired(), validators.NumberRange(min=0)])

    probability = DecimalSliderField(
        [validators.NumberRange(min=0, max=100)],
        default=90)

    airmass_limit = DecimalSliderField(
        [validators.NumberRange(min=1.0, max=5.0)],
        default=2.5)

    plan_name = TextField(
        validators=[validators.DataRequired()],
        default='REPLACE ME')

    def validate_validity_window_end(self, field):
        other = self.validity_window_start
        if field.validate(self) and other.validate(self):
            if field.data <= self.validity_window_start.data:
                raise validators.ValidationError(
                    'End time must be after start time')

    def validate_filters(self, field):
        available_filters = set(
            models.Telescope.query.get(self.telescope.data).filters)
        filters = set(re.split(r'[\s,]+', self.filters.data))

        unavailable_filters = filters - available_filters
        if unavailable_filters:
            raise validators.ValidationError(
                'Some filters are not available for this telescope: ' +
                ', '.join(unavailable_filters))

    def validate_plan_name(self, field):
        nplans = models.Plan.query.filter_by(
            dateobs=self.dateobs.data,
            telescope=self.telescope.data,
            plan_name=self.plan_name.data).count()
        if nplans > 0:
            raise validators.ValidationError('That plan name already exists.')

    def populate_obj(self, obj):
        super().populate_obj(obj)

        start_mjd = time.Time(self.validity_window_start.data).mjd
        end_mjd = time.Time(self.validity_window_end.data).mjd
        event_mjd = time.Time(self.dateobs.data).mjd
        timediff1 = start_mjd - event_mjd
        timediff2 = end_mjd - event_mjd
        t_obs = [timediff1, timediff2]
        filters = re.split(r'[\s,]+', self.filters.data)

        obj.plan_args = dict(
            localization_name=self.localization.data,
            tobs=t_obs,
            filt=filters,
            exposuretimes=[self.exposure_time.data] * len(filters),
            probability=0.01 * float(self.probability.data),
            airmass=float(self.airmass_limit.data),
            schedule_type=self.schedule.data,
            doDither=self.dither.data,
            doReferences=self.references.data,
            filterScheduleType=self.filterschedule.data,
            schedule_strategy=self.schedule_strategy.data
        )


@app.route('/event/<datetime:dateobs>/plan_new', methods=['GET', 'POST'])
@login_required
def plan_new(dateobs):

    form = PlanForm(dateobs=dateobs)
    form.telescope.choices = [
        (row.telescope,) * 2 for row in models.Telescope.query]
    form.localization.choices = [
        (row.localization_name,) * 2 for row in
        models.Localization.query.filter_by(dateobs=dateobs)]

    if request.method == 'POST':
        if form.validate():
            plan = models.Plan()
            form.populate_obj(plan)
            models.db.session.add(plan)
            models.db.session.commit()
            kwargs = dict(plan.plan_args)
            tasks.tiles.tile.delay(
                kwargs.pop('localization_name'), plan.dateobs,
                plan.telescope, plan_name=plan.plan_name,
                validity_window_start=plan.validity_window_start,
                validity_window_end=plan.validity_window_end,
                **kwargs)
            flash(
                'Started creating observing plan "{}".'.format(plan.plan_name),
                'success')
            return redirect(url_for('plan', dateobs=dateobs))

    return render_template(
        'plan_new.html', form=form, telescopes=models.Telescope.query)


class PlanManualForm(ModelForm):

    telescope = SelectField(default='ZTF')

    validity_window_start = DateTimeField(
        format='%Y-%m-%d %H:%M:%S',
        default=lambda: datetime.datetime.utcnow(),
        validators=[validators.DataRequired()])

    validity_window_end = DateTimeField(
        format='%Y-%m-%d %H:%M:%S',
        default=lambda: datetime.datetime.utcnow() + datetime.timedelta(1),
        validators=[validators.DataRequired()])

    filters = TextField('filters', validators=[validators.DataRequired()])

    dither = BooleanField(default=False)

    references = BooleanField(default=False)

    field_ids = TextField('field_ids', validators=[validators.DataRequired()])

    exposure_time = FloatField(
        default=300,
        validators=[validators.DataRequired(), validators.NumberRange(min=0)])

    queue_name = TextField(
        validators=[validators.DataRequired()],
        default='REPLACE ME')

    def validate_validity_window_end(self, field):
        other = self.validity_window_start
        if field.validate(self) and other.validate(self):
            if field.data <= self.validity_window_start.data:
                raise validators.ValidationError(
                    'End time must be after start time')

    def validate_filters(self, field):
        available_filters = set(
            models.Telescope.query.get(self.telescope.data).filters)
        filters = set(re.split(r'[\s,]+', self.filters.data))

        unavailable_filters = filters - available_filters
        if unavailable_filters:
            raise validators.ValidationError(
                'Some filters are not available for this telescope: ' +
                ', '.join(unavailable_filters))


@app.route('/plan_manual', methods=['GET', 'POST'])
@login_required
def plan_manual():

    form = PlanManualForm()
    form.telescope.choices = [
        (row.telescope,) * 2 for row in models.Telescope.query]

    if request.method == 'POST':
        if form.validate():
            telescope = form.telescope.data
            json_data, queue_name = get_json_data_manual(form)

            group(
                tasks.scheduler.submit_manual.s(
                    telescope, json_data, queue_name),
                tasks.email.compose_too.s(
                    telescope, queue_name)
            ).delay()

            flash('Submitted observing plan','success')

    return render_template(
        'plan_manual.html', form=form, telescopes=models.Telescope.query)


@app.route('/gcn_notice/<ivorn>')
@login_required
def gcn_notice(ivorn):
    ivorn = urllib.parse.unquote_plus(ivorn)
    gcn_notice = models.GcnNotice.query.get_or_404(ivorn)
    return Response(gcn_notice.content, mimetype='text/xml')


@app.route('/event/<datetime:dateobs>/localization/<localization_name>/')
@login_required
def localization(dateobs, localization_name):
    fields = models.Field.query.all()
    localization = one_or_404(
        models.Localization.query
        .filter_by(dateobs=dateobs, localization_name=localization_name))
    return render_template('localization.html', dateobs=dateobs,
                           localization=localization, fields=fields)


@app.route('/event/<datetime:dateobs>/observability/<telescope>/<localization_name>/-/airmass.png')  # noqa: E501
@login_required
def localization_airmass(dateobs, telescope, localization_name):
    return redirect(url_for(
        'localization_airmass_for_date', dateobs=dateobs, telescope=telescope,
        localization_name=localization_name, date=datetime.date.today()))


@app.route('/event/<datetime:dateobs>/observability/<telescope>/<localization_name>/<date:date>/airmass.png')  # noqa: E501
@cached_as_if_static
@login_required
def localization_airmass_for_date(dateobs, telescope, localization_name, date):
    localization = one_or_404(
        models.Localization.query
        .filter_by(dateobs=dateobs, localization_name=localization_name))
    telescope = models.Telescope.query.get_or_404(telescope)
    with \
            tempfile.NamedTemporaryFile(suffix='.fits') as fitsfile, \
            tempfile.NamedTemporaryFile(suffix='.png') as imgfile, \
            matplotlib.style.context('default'):
        io.write_sky_map(fitsfile.name, localization.table_2d, moc=True)
        plot_airmass(['--site-longitude', str(telescope.lon),
                      '--site-latitude', str(telescope.lat),
                      '--site-height', str(telescope.elevation),
                      '--site-timezone', telescope.timezone,
                      '--time', date.isoformat(),
                      fitsfile.name, '-o', imgfile.name])
        contents = imgfile.read()
    return Response(contents, mimetype='image/png')


def get_ztf_cand(url_report_page, username, password):

    '''
    Query the HTML ZTF report page to get the name & coord of the candidates
    params:
        url_report_page: can modify the date to get more specific results.
            tip: copy/paste url from ztf
        username, password : marshal GROWTH user and password
    returns:
        name_,coord: names and coordinates for the candidates
    '''

    # create a password manager
    password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()

    # Add the username and password.
    # If we knew the realm, we could use it instead of None.
    top_level_url = "http://skipper.caltech.edu:8080/"
    password_mgr.add_password(None, top_level_url, username, password)

    handler = urllib.request.HTTPBasicAuthHandler(password_mgr)

    # create "opener" (OpenerDirector instance)
    opener = urllib.request.build_opener(handler)

    # use the opener to fetch a URL
    opener.open(url_report_page)

    # Install the opener.
    # Now all calls to urllib.request.urlopen use our opener.
    urllib.request.install_opener(opener)

    with urllib.request.urlopen(url_report_page) as url:
        data = url.read().decode()

    print('Loaded :', len(data), 'ZTF objects')

    df_list = pd.read_html(data, header=0)

    coord = []
    name_ = []
    for i in range(len(df_list[1]['Name (age)'])):
        if pd.notna(df_list[1]['Name (age)'][i]):
            name_.append(df_list[1]['Name (age)'][i][:12])
            coord.append(df_list[1]['RA  Dec'][i])

    coord = np.array(coord)
    name_ = np.array(name_)

    ra_transient, dec_transient = [], []

    for i in range(len(coord)):
        c = SkyCoord(coord[i].split('+')[0], '+'+coord[i].split('+')[1],
                     unit=(u.hourangle, u.deg))
        ra_transient.append(c.ra.deg)
        dec_transient.append(c.dec.deg)

    return name_, ra_transient, dec_transient


@app.route('/event/<datetime:dateobs>/localization/<localization_name>/galaxy')
@cached_as_if_static
@login_required
def localization_galaxy(dateobs, localization_name):
    localization = one_or_404(
        models.Localization.query
        .filter_by(dateobs=dateobs, localization_name=localization_name))

    h5file = pkg_resources.resource_filename(__name__, 'catalog/CLU.hdf5')
    with h5py.File(h5file, 'r') as f:
        name = f['name'][:]
        ra, dec = f['ra'][:], f['dec'][:]
        sfr_fuv, mstar = f['sfr_fuv'][:], f['mstar'][:]
        distmpc, magb = f['distmpc'][:], f['magb'][:]
        a, b2a, pa = f['a'][:], f['b2a'][:], f['pa'][:]
        btc = f['btc'][:]

    idx = np.where(distmpc >= 0)[0]
    ra, dec = ra[idx], dec[idx]
    sfr_fuv, mstar = sfr_fuv[idx], mstar[idx]
    distmpc, magb = distmpc[idx], magb[idx]
    a, b2a, pa = a[idx], b2a[idx], pa[idx]
    btc = btc[idx]

    galaxy_coords = SkyCoord(ra * u.deg, dec * u.deg, distmpc * u.Mpc)
    prob = np.array(localization.healpix)
    nside = localization.nside
    pixarea = 4 * np.pi / len(prob)

    if localization.distmu is None:
        is3d = False
    else:
        is3d = True

    distmu = np.array(localization.distmu)
    distsigma = np.array(localization.distsigma)
    distnorm = np.array(localization.distnorm)

    # Find the posterior probability density at the position of each galaxy.
    hpx = HEALPix(nside=nside, frame='icrs')
    idx = hpx.skycoord_to_healpix(galaxy_coords)

    if is3d:
        dp_dv = norm(distmu[idx], distsigma[idx]).pdf(
                     galaxy_coords.distance.value) \
                     * prob[idx] * distnorm[idx] / pixarea
        dp_dv = np.array(dp_dv)
    else:
        dp_dv = prob[idx]

    # FIXME: Need sensible priority
    priority = dp_dv * 1.0
    priority[np.where(np.isnan(priority))[0]] = -np.inf
    idxsort = np.argsort(priority)[::-1]

    data_rows = []
    for ii in range(50):
        idx = idxsort[ii]
        c = SkyCoord(ra=ra[idx]*u.degree, dec=dec[idx]*u.degree,
                     frame='icrs')

        ra_hex = c.ra.to_string(unit=u.hour, sep=':')
        dec_hex = c.dec.to_string(unit=u.degree, sep=':')

        data_rows.append((ii+1, name[idx], ra_hex, dec_hex,
                          sfr_fuv[idx], mstar[idx],
                          distmpc[idx], magb[idx], prob[idx], priority[idx]))

    names = ('ID', 'NAME', 'RA', 'DEC', 'SFR FUV', 'Mstar', 'DIST (Mpc)',
             'MAG', 'PROB', 'PRIORITY')

    if not data_rows:
        galaxy_table = ""
    else:
        t = Table(rows=data_rows, names=names)
        old_stdout = sys.stdout
        sys.stdout = mystdout = StringIO()
        t.write(format='ascii.csv', delimiter=',', filename=sys.stdout,
                formats={'SFR FUV': '%.3e', 'Mstar': '%.3e',
                         'DIST (Mpc)': '%.1f', 'MAG': '%.1f',
                         'PROB': '%.3e', 'PRIORITY': '%.3e'})
        sys.stdout = old_stdout
        galaxy_table = "\n".join(mystdout.getvalue().split("\n")[1:])

    return render_template('galaxy.html', dateobs=dateobs,
                           localization=localization,
                           galaxy_table=galaxy_table)


@app.route('/event/<datetime:dateobs>/localization/<localization_name>/json')
@cached_as_if_static
@login_required
def localization_json(dateobs, localization_name):
    localization = one_or_404(
        models.Localization.query
        .filter_by(dateobs=dateobs, localization_name=localization_name))
    if localization.contour is None:
        abort(404)
    return jsonify(localization.contour)


def get_queue_transient_name(plan):

    event = models.Event.query.filter_by(dateobs=plan.dateobs).one()
    gcn_notice = event.gcn_notices[-1]
    stream = gcn_notice.stream

    queue_name = "{0}_{1}_{2}_{3}".format(str(plan.dateobs).replace(" ","-"),
                                          plan.plan_name,
                                          str(plan.validity_window_start).replace(" ","-"),
                                          str(plan.validity_window_end).replace(" ","-"))

    transient_name = "{0}_{1}".format(stream,
                                      str(plan.dateobs).replace(" ", "-"))

    return queue_name, transient_name


def get_json_data_manual(form):

    telescope = form.telescope.data
    filters = form.filters.data.split(",")
    field_ids = [int(x) for x in form.field_ids.data.split(",")]
    doDither = bool(form.dither.data)
    doReferences = bool(form.references.data)
    exposure_time = form.exposure_time.data
    queue_name = form.queue_name.data

    start_mjd = time.Time(form.validity_window_start.data).mjd
    end_mjd = time.Time(form.validity_window_end.data).mjd

    program_pis = {'ZTF': 'Kulkarni', 'DECam': 'Andreoni/Goldstein',
                   'Gattini': 'Kasliwal', 'KPED': 'Coughlin',
                   'GROWTH-India': 'Bhalerao'}

    program_id = 2
    bands = {'g': 1, 'r': 2, 'i': 3, 'z': 4, 'J': 5}
    json_data = {'queue_name': "ToO_"+queue_name,
                 'validity_window_mjd': [start_mjd, end_mjd]}
    targets = []
    cnt = 1
    for filt in filters:            
        filter_id = bands[filt]
        for field_id in field_ids:
            field = one_or_404(models.Field.query.filter_by(
                telescope=telescope, field_id=field_id))
            if doReferences and filter_id not in field.reference_filter_ids:
                continue

            target = {'request_id': cnt,
                      'program_id': program_id,
                      'field_id': field_id,
                      'ra': field.ra,
                      'dec': field.dec,
                      'filter_id': filter_id,
                      'exposure_time': exposure_time,
                      'program_pi': program_pis[telescope],
                      'subprogram_name': "ToO_manual"
                      }
            targets.append(target)
            cnt = cnt + 1

    json_data['targets'] = targets

    if telescope == "DECam":
        decam_dicts = []
        cnt = 1
        queue_name = json_data['queue_name']
        if doDither:
            nrows = 2*len(json_data['targets'])
        else:
            nrows = len(json_data['targets'])

        for ii, data_row in enumerate(json_data['targets']):
            if doDither:
                # handle the DECam dithers
                for jj in [0, 1]:
                    if jj == 0:
                        ra_diff, dec_diff = 0.0, 0.0
                    elif jj == 1:
                        ra_diff, dec_diff = 60.0/3600.0, 60.0/3600.0
                    decam_dict = tasks.scheduler.get_decam_dict(data_row,
                                                                queue_name,
                                                                cnt, nrows,
                                                                ra_diff=ra_diff,
                                                                dec_diff=dec_diff)
                    decam_dicts.append(decam_dict)
                    cnt = cnt + 1
            else:
                decam_dict = tasks.scheduler.get_decam_dict(data_row,
                                                            queue_name,
                                                            cnt, nrows)
                decam_dicts.append(decam_dict)
                cnt = cnt + 1
        json_data = decam_dicts

    return json_data, queue_name


def get_json_data(plan):

    queue_name, transient_name = get_queue_transient_name(plan)

    start_mjd = time.Time(plan.validity_window_start).mjd
    end_mjd = time.Time(plan.validity_window_end).mjd
    exposures = plan.planned_observations
    telescope = plan.telescope
    doReferences = plan.plan_args["doReferences"]
    doDither = plan.plan_args["doDither"]

    program_pis = {'ZTF': 'Kulkarni', 'DECam': 'Andreoni/Goldstein',
                   'Gattini': 'Kasliwal', 'KPED': 'Coughlin',
                   'GROWTH-India': 'Bhalerao'}

    if doDither:
        ditherNorm = 2.0
    else:
        ditherNorm = 1.0

    if doReferences:
        json_data = {
            'queue_name': "ToO_"+queue_name,
            'validity_window_mjd': [start_mjd, end_mjd],
            'targets': [
                {
                    'request_id': ii,
                    'program_id': plan.program_id,
                    'field_id': exposure.field_id,
                    'ra': exposure.field.ra,
                    'dec': exposure.field.dec,
                    'filter_id': exposure.filter_id,
                    'exposure_time': exposure.exposure_time/ditherNorm,
                    'program_pi': program_pis[telescope],
                    'subprogram_name': "ToO_"+transient_name
                }
                for ii, exposure in enumerate(exposures)
                if exposure.filter_id in
                exposure.field.reference_filter_ids
            ]
        }
    else:
        json_data = {
            'queue_name': "ToO_"+queue_name,
            'validity_window_mjd': [start_mjd, end_mjd],
            'targets': [
                {
                    'request_id': ii,
                    'program_id': plan.program_id,
                    'field_id': exposure.field_id,
                    'ra': exposure.field.ra,
                    'dec': exposure.field.dec,
                    'filter_id': exposure.filter_id,
                    'exposure_time': exposure.exposure_time/ditherNorm,
                    'program_pi': program_pis[telescope],
                    'subprogram_name': "ToO_"+transient_name
                }
                for ii, exposure in enumerate(exposures)
            ]
        }

    if telescope == "DECam":
        decam_dicts = []
        cnt = 1
        queue_name = json_data['queue_name']
        if doDither:
            nrows = 2*len(json_data['targets'])
        else:
            nrows = len(json_data['targets'])

        for ii, data_row in enumerate(json_data['targets']):
            if doDither:
                # handle the DECam dithers
                for jj in [0, 1]:
                    if jj == 0:
                        ra_diff, dec_diff = 0.0, 0.0
                    elif jj == 1:
                        ra_diff, dec_diff = 60.0/3600.0, 60.0/3600.0
                    decam_dict = tasks.scheduler.get_decam_dict(data_row,
                                                                queue_name,
                                                                cnt, nrows,
                                                                ra_diff=ra_diff,
                                                                dec_diff=dec_diff)
                    decam_dicts.append(decam_dict)
                    cnt = cnt + 1
            else:
                decam_dict = tasks.scheduler.get_decam_dict(data_row,
                                                            queue_name,
                                                            cnt, nrows)
                decam_dicts.append(decam_dict)
                cnt = cnt + 1
        json_data = decam_dicts

    return json_data, queue_name


def get_total_probability(telescope, exposures):

    total_probability = 0.0
    field_ids = []
    for ii, exposure in enumerate(exposures):
        field_id = exposure.field.field_id
        if field_id in field_ids:
            continue
        field_ids.append(field_id)

        if telescope in ["ZTF", "DECam"]:
            if exposure.filter_id in exposure.field.reference_filter_ids:
                total_probability = total_probability + exposure.weight
        else:
            total_probability = total_probability + exposure.weight

    return total_probability


def get_filt_probs(tiles):

    filt_probs = np.array([0.0, 0.0, 0.0, 0.0])
    for tile in tiles:
        for ii, filter_id in enumerate([1, 2, 3, 4]):
            if filter_id in tile.field.reference_filter_ids:
                filt_probs[ii] = filt_probs[ii] + tile.probability
    filt_probs_string = "g: %.2f r: %.2f i: %.2f z: %.2f" %\
        (filt_probs[0], filt_probs[1], filt_probs[2], filt_probs[3])

    return filt_probs_string


def get_filters_string(telescopes):

    filters_string = []
    for telescope in telescopes:
        filters_string.append("%s: %s" % (telescope.telescope,
                                          ", ".join(telescope.filters)))
    return " ".join(filters_string)


@app.route('/telescope/<telescope>/field/<int:field_id>/json')
@cached_as_if_static
def field_json(telescope, field_id):
    field = one_or_404(models.Field.query.filter_by(
        telescope=telescope, field_id=field_id))
    return jsonify(field.contour)


class UserForm(ModelForm):
    class Meta:
        model = models.User


@app.route('/user', methods=['GET', 'POST'])
@login_required
def user():
    form = UserForm(obj=current_user)
    if request.method == 'POST':
        was_validated = True
        if form.validate():
            form.populate_obj(current_user)
            models.db.session.add(current_user)
            models.db.session.commit()
            flash('User profile updated', 'success')
            return redirect('/')
    else:
        was_validated = False
    return render_template('user.html', user=current_user, form=form,
                           was_validated=was_validated)


@app.route('/user/test', methods=['POST'])
@login_required
def user_test():
    if current_user.phone:
        tasks.twilio.text_for.delay(
            render_template('user_test.txt'), current_user.phone)
        if current_user.voice:
            tasks.twilio.call_for.delay(
                'user_test_voice_twiml', current_user.phone)
        flash('A test alert was sent to {}. You should '
              'receive it momentarily if the server is correctly '
              'configured.'.format(current_user.phone), 'success')
        return redirect(url_for('index'))
    else:
        flash('You have not yet set up your phone number.', 'danger')
        return redirect(url_for('user'))


@app.route('/health/telescope/<telescope>')
def health_queue(telescope):
    """Check connectivity with the ZTF queue.
    Returns an HTTP 204 No Content response on success,
    or an HTTP 500 Internal Server Error response on failure.
    """
    tasks.scheduler.ping.delay(telescope).get(10)
    return '', 204  # No Content


@app.route('/health/growth-marshal')
def health_growth_marshal():
    """Check connectivity with the GROWTH Marshal.
    Returns an HTTP 204 No Content response on success,
    or an HTTP 500 Internal Server Error response on failure.
    """
    from . import growthdb
    return '', 204  # No Content


@app.route('/health')
@login_required
def health():
    return render_template('health.html', telescopes=models.Telescope.query)
