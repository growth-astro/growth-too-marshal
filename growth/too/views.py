import warnings
import datetime
import json
import os
import urllib.parse
import math
import re
import requests
import shutil
import tempfile

from celery import group
import numpy as np
from astropy import time
import astropy.units as u
from astropy.coordinates import SkyCoord
import pandas as pd
from ligo.skymap import io
from ligo.skymap.postprocess import find_injection_moc
from ligo.skymap.tool.ligo_skymap_plot_airmass import main as plot_airmass
from ligo.skymap.tool.ligo_skymap_plot_observability import main \
    as plot_observability
import matplotlib.style

from flask import (
    abort, flash, jsonify, make_response, redirect, render_template, request,
    Response, url_for)
from flask_caching import Cache
from flask_login import (
    current_user, login_required, login_user, logout_user, LoginManager)
from wtforms import BooleanField, FloatField, RadioField, TextField
from wtforms_components.fields import (
    DateTimeField, DecimalSliderField, SelectField)
from wtforms import validators
from passlib.apache import HtpasswdFile
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from .flask import app
from .jinja import atob
from . import catalogs, models, tasks

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

# Server-side cache for rendered view functions.
cache = Cache(app, config={
    'CACHE_DEFAULT_TIMEOUT': 86400,
    'CACHE_REDIS_HOST': tasks.celery.backend.client,
    'CACHE_TYPE': 'redis'})


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
    else:
        event = models.Event.query.filter_by(dateobs=dateobs).all()
        if len(event) == 0 or event is None:
            return 'None'
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
    tags = sorted(
        tag for tag, in models.db.session.query(models.Tag.text).distinct())
    return render_template(
        'index.html',
        tags=tags,
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


@app.route('/event/<datetime:dateobs>/localization/<localization_name>/plan/telescope/<telescope>/<plan_name>/gcn')  # noqa: E501
@cache.cached()
def create_gcn_template(dateobs, telescope, localization_name, plan_name):

    authors = ["Fred Zwicky", "Albert Einstein"]

    localization = one_or_404(models.Localization.query.filter_by(
        dateobs=dateobs, localization_name=localization_name))
    plan = one_or_404(models.Plan.query.filter_by(dateobs=dateobs,
                                                  telescope=telescope,
                                                  plan_name=plan_name))
    if plan.status < plan.Status.READY:
        abort(404)
    tdiff = human_time((time.Time(plan.validity_window_start, scale='utc')
                        - time.Time(dateobs, scale='utc')).value)

    return render_template('gcn.jinja2', plan=plan, tdiff=tdiff,
                           authors=authors, localization=localization)


@app.route('/event/<datetime:dateobs>/plan/download/telescope/<telescope>/<plan_name>.json')  # noqa: E501
@cache.cached()
def download_json(dateobs, telescope, plan_name):

    plan = one_or_404(models.Plan.query.filter_by(
        dateobs=dateobs, telescope=telescope, plan_name=plan_name))
    if plan.status < plan.Status.READY:
        abort(404)
    json_data, queue_name = get_json_data(plan)

    # FIXME: reformat for DECam.
    # Should update Gattini, KPED, and GROWTH-India parsers instead.
    if telescope == 'DECam':
        scalar_jsondata = []
        for d in json_data:
            scalar_d = {}
            for key in d:
                if isinstance(d[key], tuple):
                    scalar_d[key] = d[key][0]
                else:
                    scalar_d[key] = d[key]
            scalar_jsondata.append(scalar_d)
        json_data = scalar_jsondata

    return jsonify(json_data)


@app.route('/event/<datetime:dateobs>/plan/telescope/<telescope>/<plan_name>/json')  # noqa: E501
@cache.cached()
def plan_json(dateobs, telescope, plan_name):
    plan = one_or_404(models.Plan.query.filter_by(
        dateobs=dateobs, telescope=telescope, plan_name=plan_name))
    if plan.status < plan.Status.READY:
        abort(404)
    return jsonify([
        planned_observation.field_id
        for planned_observation in plan.planned_observations
    ])


@app.route('/event/<datetime:dateobs>/plan/telescope/<telescope>/<plan_name>/prob')  # noqa: E501
@cache.cached()
def prob_json(dateobs, telescope, plan_name):
    localization_name = request.args.get('localization_name')
    plan = one_or_404(models.Plan.query.filter_by(
        dateobs=dateobs, telescope=telescope, plan_name=plan_name))
    if plan.status < plan.Status.READY:
        abort(404)
    localization = one_or_404(models.Localization.query.filter_by(
        dateobs=dateobs, localization_name=localization_name))
    prob = plan.get_probability(localization)
    return jsonify(prob)


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
                 ('airmass_weighted', 'airmass_weighted'),
                 ('greedy_slew', 'greedy_slew')],
        default='greedy')

    dither = BooleanField(default=False)

    references = BooleanField(default=False)

    primary = BooleanField(default=False)

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

    mindiff = DecimalSliderField(
        [validators.NumberRange(min=0, max=180)],
        default=30)

    plan_name = TextField(
        validators=[validators.DataRequired()],
        default='REPLACE ME')

    previous = BooleanField(default=False)

    previous_plan = SelectField()

    def _previous_plan_query(self):
        return models.Plan.query.filter_by(dateobs=self.dateobs.data)

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
            mindiff=float(self.mindiff.data),
            schedule_type=self.schedule.data,
            doDither=self.dither.data,
            doReferences=self.references.data,
            doUsePrimary=self.primary.data,
            filterScheduleType=self.filterschedule.data,
            schedule_strategy=self.schedule_strategy.data,
            usePrevious=self.previous.data,
            previous_plan=self.previous_plan.data
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
    form.previous_plan.choices = [
        ("%s-%s" % (row.telescope, row.plan_name),) * 2 for row in
        models.Plan.query.filter_by(dateobs=dateobs)]

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

            flash('Submitted observing plan', 'success')

    return render_template(
        'plan_manual.html', form=form, telescopes=models.Telescope.query)


@app.route('/gcn_notice/<ivorn>')
@login_required
@cache.cached()
def gcn_notice(ivorn):
    ivorn = urllib.parse.unquote_plus(ivorn)
    gcn_notice = models.GcnNotice.query.get_or_404(ivorn)
    return Response(gcn_notice.content, mimetype='text/xml')


@app.route(
    '/event/<datetime:dateobs>/localization/<localization_name>',
    methods=['POST'])
@login_required
def localization_post(dateobs, localization_name):
    """
    Manual FITS file upload::

        $ curl -c cookie.txt -d "user=XXXX" -d "password=XXXX" http://example.edu/login
        $ curl -b cookie.txt --data-binary @/path/to/skymap.fits http://example.edu/event/YYYY-MM-DDTHH:MM:SS/localization/bayestar.fits
        $ rm cookie.txt

    FIXME: figure out how to use HTTP Basic auth or session auth transparently.
    """  # noqa: E501
    with tempfile.NamedTemporaryFile(suffix=localization_name) as localfile:
        shutil.copyfileobj(request.stream, localfile)
        localfile.flush()
        skymap = io.read_sky_map(localfile.name, moc=True)

    def get_col(m, name):
        try:
            col = m[name]
        except KeyError:
            return None
        else:
            return col.tolist()

    models.db.session.add(
        models.Localization(
            localization_name=localization_name,
            dateobs=dateobs,
            uniq=get_col(skymap, 'UNIQ'),
            probdensity=get_col(skymap, 'PROBDENSITY'),
            distmu=get_col(skymap, 'DISTMU'),
            distsigma=get_col(skymap, 'DISTSIGMA'),
            distnorm=get_col(skymap, 'DISTNORM')))

    models.db.session.commit()
    tasks.skymaps.contour.delay(localization_name, dateobs)
    return '', 201


@app.route('/event/<datetime:dateobs>/observability/-/<localization_name>/-/observability.png')  # noqa: E501
@login_required
def localization_observability(dateobs, localization_name):
    return redirect(url_for(
        'localization_observability_for_date', dateobs=dateobs,
        localization_name=localization_name, date=datetime.date.today()))


@app.route('/event/<datetime:dateobs>/observability/-/<localization_name>/<date:date>/observability.png')  # noqa: E501
@login_required
@cache.cached()
def localization_observability_for_date(dateobs, localization_name, date):
    localization = one_or_404(
        models.Localization.query
        .filter_by(dateobs=dateobs, localization_name=localization_name))
    names, lons, lats, heights = zip(*(
        (t.telescope, str(t.lon), str(t.lat), str(t.elevation))
        for t in models.Telescope.query))
    with \
            tempfile.NamedTemporaryFile(suffix='.fits') as fitsfile, \
            tempfile.NamedTemporaryFile(suffix='.png') as imgfile, \
            matplotlib.style.context('default'):
        io.write_sky_map(fitsfile.name, localization.table_2d, moc=True)
        plot_observability(['--site-name', *names,
                            '--site-longitude', *lons,
                            '--site-latitude', *lats,
                            '--site-height', *heights,
                            '--time', date.isoformat(),
                            fitsfile.name, '-o', imgfile.name])
        contents = imgfile.read()
    return Response(contents, mimetype='image/png')


@app.route('/event/<datetime:dateobs>/observability/<telescope>/<localization_name>/-/airmass.png')  # noqa: E501
@login_required
def localization_airmass(dateobs, telescope, localization_name):
    return redirect(url_for(
        'localization_airmass_for_date', dateobs=dateobs, telescope=telescope,
        localization_name=localization_name, date=datetime.date.today()))


@app.route('/event/<datetime:dateobs>/observability/<telescope>/<localization_name>/<date:date>/airmass.png')  # noqa: E501
@login_required
@cache.cached()
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


@app.route('/event/<datetime:dateobs>/localization/<localization_name>/json')
@login_required
@cache.cached()
def localization_json(dateobs, localization_name):
    localization = one_or_404(
        models.Localization.query
        .filter_by(dateobs=dateobs, localization_name=localization_name))
    if localization.contour is None:
        abort(404)
    return jsonify(localization.contour)


def nan_to_none(o):
    if o != o:
        return None
    else:
        return o


@app.route('/event/<datetime:dateobs>/galaxies/json')
# @login_required
def galaxies_data(dateobs):
    event = models.Event.query.get_or_404(dateobs)
    table = catalogs.galaxies.copy()

    # Populate 2D and 3D credible levels.
    localization_name = request.args.get('search[value]')
    localization = models.Localization.query.filter_by(
        dateobs=event.dateobs, localization_name=localization_name).one_or_none() or event.localizations[-1]
    results = find_injection_moc(
        localization.table,
        np.deg2rad(table['ra']),
        np.deg2rad(table['dec']),
        table['distmpc'])
    table['2D CL'] = results.searched_prob
    table['3D CL'] = results.searched_prob_vol

    result = {}

    # Populate total number of records.
    result['recordsTotal'] = len(table)

    # Populate draw counter.
    try:
        value = int(request.args['draw'])
    except KeyError:
        pass
    except ValueError:
        abort(400)
    else:
        result['draw'] = value

    for i in range(len(table.columns)):
        try:
            value = json.loads(
                request.args['columns[{}][search][value]'.format(i)] or '{}'
            )
        except (KeyError, ValueError):
            pass
        else:
            try:
                value2, = np.asarray([value['min']], dtype=table[table.colnames[i]].dtype)
            except KeyError:
                pass
            except ValueError:
                abort(400)
            else:
                table = table[table[table.colnames[i]] >= value2]

            try:
                value2, = np.asarray([value['max']], dtype=table[table.colnames[i]].dtype)
            except (KeyError, ValueError):
                pass
            else:
                table = table[table[table.colnames[i]] <= value2]

        try:
            value = int(request.args['order[{}][column]'.format(i)])
        except (KeyError, ValueError):
            pass
        else:
            table.sort(table.colnames[value])

        try:
            value = request.args['order[{}][dir]'.format(i)]
        except (KeyError, ValueError):
            pass
        else:
            if value == 'desc':
                table.reverse()

    # Populate total number of filtered records.
    result['recordsFiltered'] = len(table)

    # Trim results by requested start index.
    try:
        value = int(request.args['start'])
    except KeyError:
        pass
    except ValueError:
        abort(400)
    else:
        table = table[value:]

    # Trim results by requested length.
    try:
        value = int(request.args['length'])
    except KeyError:
        pass
    except ValueError:
        abort(400)
    else:
        table = table[:value]

    result['data'] = [[nan_to_none(col) for col in row]
                      for row in table.as_array().tolist()]

    return jsonify(result)


@app.route('/event/<datetime:dateobs>/galaxies')
# @login_required
def galaxies(dateobs):
    event = models.Event.query.get_or_404(dateobs)
    return render_template(
        'galaxies.html', event=event, table=catalogs.galaxies)


def get_queue_transient_name(plan):

    event = models.Event.query.filter_by(dateobs=plan.dateobs).one()
    gcn_notice = event.gcn_notices[-1]
    stream = gcn_notice.stream

    queue_name = "{0}_{1}_{2}_{3}".format(
        str(plan.dateobs).replace(" ", "-"),
        plan.plan_name,
        str(plan.validity_window_start).replace(" ", "-"),
        str(plan.validity_window_end).replace(" ", "-"))

    transient_name = "{0}_{1}".format(
        stream, str(plan.dateobs).replace(" ", "-"))

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
    json_data = {'queue_name': "ToO_" + queue_name,
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
                    decam_dict = tasks.scheduler.get_decam_dict(
                        data_row, queue_name, cnt, nrows,
                        ra_diff=ra_diff, dec_diff=dec_diff)
                    decam_dicts.append(decam_dict)
                    cnt = cnt + 1
            else:
                decam_dict = tasks.scheduler.get_decam_dict(
                    data_row, queue_name, cnt, nrows)
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
                    decam_dict = tasks.scheduler.get_decam_dict(
                        data_row, queue_name, cnt, nrows,
                        ra_diff=ra_diff, dec_diff=dec_diff)
                    decam_dicts.append(decam_dict)
                    cnt = cnt + 1
            else:
                decam_dict = tasks.scheduler.get_decam_dict(
                    data_row, queue_name, cnt, nrows)
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
    from . import growthdb  # noqa: F401
    return '', 204  # No Content


@app.route('/health')
@login_required
def health():
    return render_template('health.html', telescopes=models.Telescope.query)
