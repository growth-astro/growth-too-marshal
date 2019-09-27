import datetime
from unittest import mock

from astropy import time
from astropy import units as u
import gcn
import lxml.etree
import numpy as np
import pkg_resources
import pytest

from .. import models
from ..jinja import btoa
from ..flask import app
from ..gcn import handle, listen
from . import mock_download_file


@pytest.mark.freeze_time('2017-08-17')
def test_freeze_time():
    """Test that freezing time works."""
    assert datetime.date.today() == datetime.date(2017, 8, 17)
    assert datetime.datetime.now() == datetime.datetime(2017, 8, 17)
    assert time.Time.now() == time.Time('2017-08-17')


@mock.patch('growth.too.tasks.skymaps.contour.run')
@mock.patch('growth.too.tasks.tiles.tile.run')
@mock.patch('growth.too.tasks.skymaps.from_cone.run')
def test_grb180116a_gnd_pos(mock_from_cone, mock_tile, mock_contour,
                            celery, flask, mail):
    # Read test GCN
    payload = pkg_resources.resource_string(
        __name__, 'data/GRB180116A_Fermi_GBM_Gnd_Pos.xml')
    root = lxml.etree.fromstring(payload)

    # Run function under test
    handle(payload, root)

    # Check that we didn't write the unhelpful "unknown" short/long class
    dateobs = '2018-01-16T00:36:53'
    event = models.Event.query.get(dateobs)
    assert event.tags == ['Fermi', 'GRB']


@mock.patch('growth.too.tasks.skymaps.contour.run')
@mock.patch('growth.too.tasks.twilio.call_everyone.run')
@mock.patch('growth.too.tasks.slack.slack_everyone.run')
@mock.patch('astropy.io.fits.file.download_file', mock_download_file)
@pytest.mark.freeze_time('2019-08-21')
def test_grb180116a_fin_pos(mock_call_everyone, mock_slack_everyone,
                            mock_contour,
                            celery, flask, mail):
    # Read test GCN
    payload = pkg_resources.resource_string(
        __name__, 'data/GRB180116A_Fermi_GBM_Fin_Pos.xml')
    root = lxml.etree.fromstring(payload)

    # Run function under test
    handle(payload, root)

    dateobs = '2018-01-16T00:36:53'
    event = models.Event.query.get(dateobs)
    assert event is not None
    *_, gcn_notice = event.gcn_notices
    assert gcn_notice.content == payload
    assert gcn_notice.notice_type == gcn.NoticeType.FERMI_GBM_FIN_POS
    assert time.Time(gcn_notice.date) == time.Time('2018-01-16T00:46:05')
    assert gcn_notice.ivorn == 'ivo://nasa.gsfc.gcn/Fermi#GBM_Fin_Pos2018-01-16T00:36:52.81_537755817_0-026'  # noqa: E501
    assert gcn_notice.stream == 'Fermi'
    assert time.Time(gcn_notice.dateobs) - time.Time(dateobs) < 0.5 * u.second
    assert event.tags == ['Fermi', 'long', 'GRB']

    mock_call_everyone.assert_not_called()
    mock_slack_everyone.assert_not_called()

    localization, = event.localizations
    assert np.isclose(localization.flat_2d.sum(), 1.0)

    telescope = 'ZTF'
    filt = ['g', 'r', 'g']
    exposuretimes = [300.0, 300.0, 300.0]
    doReferences, doDither = True, False
    filterScheduleType = 'block'
    schedule_type = 'greedy'
    probability = 0.9
    plan_name = "%s_%s_%s_%d_%d_%s_%d_%d" % (localization.localization_name,
                                             "".join(filt), schedule_type,
                                             doDither, doReferences,
                                             filterScheduleType,
                                             exposuretimes[0],
                                             100*probability)
    plan = models.Plan.query.filter_by(plan_name=plan_name,
                                       telescope=telescope).one()

    assert time.Time(plan.dateobs) - time.Time(dateobs) < 0.5 * u.second

    exposures = models.PlannedObservation.query.filter_by(
            dateobs=event.dateobs,
            telescope=telescope,
            plan_name=plan.plan_name).all()

    for exposure in exposures:
        field_id = exposure.field_id
        assert np.all(np.array(field_id) < 2000)
        assert np.all(np.array(exposure.exposure_time) > 0)
        assert np.all(np.array(exposure.weight) <= 1)

    assert np.isclose(plan.area, 651.6459456904389)

    # Try submitting some of the observing plans.
    flask.post(
        '/event/{}/plan'.format(dateobs),
        data={
            'go': True,
            '{}_{}'.format(btoa(telescope), btoa(plan_name)): True
        }
    )


@mock.patch('growth.too.tasks.skymaps.contour.run')
@mock.patch('growth.too.tasks.tiles.tile.run')
@mock.patch('growth.too.tasks.skymaps.from_cone.run')
@mock.patch('growth.too.tasks.skymaps.download.run')
def test_grb180116a_multiple_gcns(mock_download, mock_from_cone, mock_tile,
                                  mock_contour, celery, flask, mail):
    """Test reading and ingesting all three GCNs. Make sure that there are
    no database conflicts."""
    for notice_type in ['Alert', 'Flt_Pos', 'Gnd_Pos', 'Fin_Pos']:
        filename = 'data/GRB180116A_Fermi_GBM_' + notice_type + '.xml'
        payload = pkg_resources.resource_string(__name__, filename)
        root = lxml.etree.fromstring(payload)
        handle(payload, root)


@mock.patch.dict(app.jinja_env.globals,
                 {'now': lambda: time.Time('2018-04-22T21:55:30').datetime})
@mock.patch('growth.too.tasks.twilio.text_everyone.run')
@mock.patch('growth.too.tasks.twilio.call_everyone.run')
@mock.patch('growth.too.tasks.slack.slack_everyone.run')
@mock.patch('growth.too.tasks.skymaps.contour.run')
@mock.patch('growth.too.tasks.tiles.tile.run')
@mock.patch('growth.too.tasks.skymaps.from_cone.run')
@mock.patch('astropy.io.fits.file.download_file', mock_download_file)
def test_gbm_subthreshold(mock_from_cone, mock_tile, mock_contour,
                          mock_call_everyone, mock_text_everyone,
                          mock_slack_everyone, celery,
                          flask, mail):
    """Test reading and ingesting all three GCNs. Make sure that there are
    no database conflicts."""
    filename = 'data/GRB180422.913_Subthreshold.xml'
    payload = pkg_resources.resource_string(__name__, filename)
    root = lxml.etree.fromstring(payload)
    handle(payload, root)

    event = models.Event.query.get('2018-04-22T21:54:11')
    assert event is not None
    gcn_notice, = event.gcn_notices
    assert gcn_notice.notice_type == gcn.NoticeType.FERMI_GBM_SUBTHRESH
    assert gcn_notice.stream == 'Fermi'
    assert event.tags == ['Fermi', 'short', 'transient']

    mock_text_everyone.assert_not_called()
    mock_call_everyone.assert_not_called()
    mock_slack_everyone.assert_not_called()


@mock.patch('growth.too.tasks.skymaps.contour.run')
@mock.patch('growth.too.tasks.tiles.tile.run')
@mock.patch('growth.too.tasks.skymaps.from_cone.run')
def test_amon_151115(mock_from_cone, mock_tile, mock_contour,
                     celery, flask, mail):
    # Read test GCN
    payload = pkg_resources.resource_string(
        __name__, 'data/AMON_151115.xml')
    root = lxml.etree.fromstring(payload)

    # Run function under test
    handle(payload, root)

    dateobs = '2015-11-15T11:53:44'
    event = models.Event.query.get(dateobs)
    assert event.tags == ['AMON']


@mock.patch('growth.too.tasks.skymaps.contour.run')
@mock.patch('growth.too.tasks.tiles.tile.run')
@mock.patch('growth.too.tasks.skymaps.from_cone.run')
def test_amon_icecube_gold_190730(mock_from_cone, mock_tile, mock_contour,
                                  celery, flask, mail):
    # Read test GCN
    payload = pkg_resources.resource_string(
        __name__, 'data/AMON_ICECUBE_GOLD_190730.xml')
    root = lxml.etree.fromstring(payload)

    # Run function under test
    handle(payload, root)

    dateobs = '2019-07-30T20:50:41'
    event = models.Event.query.get(dateobs)
    assert event.tags == ['AMON']


@mock.patch('growth.too.tasks.skymaps.contour.run')
@mock.patch('growth.too.tasks.tiles.tile.run')
@mock.patch('growth.too.tasks.skymaps.from_cone.run')
def test_amon_icecube_bronze_190819(mock_from_cone, mock_tile, mock_contour,
                                    celery, flask, mail):
    # Read test GCN
    payload = pkg_resources.resource_string(
        __name__, 'data/AMON_ICECUBE_BRONZE_190819.xml')
    root = lxml.etree.fromstring(payload)

    # Run function under test
    handle(payload, root)

    dateobs = '2019-08-19T17:34:24'
    event = models.Event.query.get(dateobs)
    assert event.tags == ['AMON']


@mock.patch('gcn.listen')
def test_listen(mock_listen):
    # Run function under test
    listen()

    # Check that GCN listener was invoked
    assert mock_listen.called_once_with(handle=handle)
