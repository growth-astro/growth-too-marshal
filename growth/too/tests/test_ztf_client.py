from unittest.mock import Mock, MagicMock

from astropy.table import Table
from astropy import time
import pkg_resources
import pytest

from .. import models
from ..tasks import ztf_client


@pytest.fixture
def mock_refstable():
    filename = 'data/ztf_ref_table.dat'
    with pkg_resources.resource_stream(__name__, filename) as f:
        return Table.read(f, format='ascii')


@pytest.fixture
def mock_refclient(monkeypatch, mock_refstable):
    client = Mock(**{
        'search.return_value.to_table.return_value': mock_refstable})
    monkeypatch.setattr('growth.too.tasks.ztf_client.client', client)
    return client


@pytest.fixture
def mock_get_ztf_depot_table_ref(monkeypatch, mock_refstable):
    get_ztf_depot_table = MagicMock(**{'return_value': mock_refstable})
    monkeypatch.setattr('growth.too.tasks.ztf_client.get_ztf_depot_table',
                        get_ztf_depot_table)
    return get_ztf_depot_table


def test_refs(mock_get_ztf_depot_table_ref):
    ztf_client.ztf_references()
    field = models.Field.query.filter_by(telescope='ZTF', field_id=324).one()
    assert field.reference_filter_ids == [1]
    assert field.reference_filter_mags == [21.326900]


@pytest.fixture
def mock_obstable():
    filename = 'data/ztf_obs_table.dat'
    with pkg_resources.resource_stream(__name__, filename) as f:
        return Table.read(f, format='ascii')


@pytest.fixture
def mock_obsclient(monkeypatch, mock_obstable):
    client = Mock(**{
        'search.return_value.to_table.return_value': mock_obstable})
    monkeypatch.setattr('growth.too.tasks.ztf_client.client', client)
    return client


def test_obs(mock_obsclient):
    ztf_client.ztf_obs()
    observation = models.Observation.query.filter_by(telescope='ZTF',
                                                     observation_id=84218480,
                                                     subfield_id=54).one()
    assert observation.field_id == 789
    assert observation.limmag == 20.895300


@pytest.fixture
def mock_deptable():
    filename = 'data/ztf_depot_table.dat'
    with pkg_resources.resource_stream(__name__, filename) as f:
        return Table.read(f, format='ascii.fixed_width',
                          data_start=2, data_end=-1)


@pytest.fixture
def mock_get_ztf_depot_table(monkeypatch, mock_deptable):
    get_ztf_depot_table = MagicMock(**{'return_value': mock_deptable})
    monkeypatch.setattr('growth.too.tasks.ztf_client.get_ztf_depot_table',
                        get_ztf_depot_table)
    return get_ztf_depot_table


def test_depot(mock_get_ztf_depot_table):
    start_time = time.Time('2019-09-25T00:00:00',
                           format='isot', scale='utc')
    end_time = time.Time('2019-09-26T00:00:00',
                         format='isot', scale='utc')

    ztf_client.ztf_depot(start_time=start_time, end_time=end_time)
    observation = models.Observation.query.filter_by(telescope='ZTF',
                                                     observation_id=99753019,
                                                     subfield_id=45).one()
    assert observation.field_id == 518
    assert observation.limmag == 19.6
