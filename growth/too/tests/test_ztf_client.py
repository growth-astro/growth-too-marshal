from unittest.mock import Mock

from astropy.table import Table
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
def mock_obsstable():
    filename = 'data/ztf_obs_table.dat'
    with pkg_resources.resource_stream(__name__, filename) as f:
        return Table.read(f, format='ascii')


@pytest.fixture
def mock_client(monkeypatch, mock_refstable):
    client = Mock(**{
        'search.return_value.to_table.return_value': mock_refstable})
    monkeypatch.setattr('growth.too.tasks.ztf_client.client', client)
    return client


def test_refs(mock_client):
    ztf_client.ztf_references()
    field = models.Field.query.filter_by(telescope='ZTF', field_id=324).one()
    assert field.reference_filter_ids == [1]
    assert field.reference_filter_mags == [21.326900]


def test_obs(mock_client):
    ztf_client.ztf_obs()
    observation = models.Observation.query.filter_by(telescope='ZTF',
                                                     observation_id=84218480,
                                                     subfield_id=54).one()
    assert observation.field_id == 789
    assert observation.limmag == 20.895300
