from unittest.mock import Mock

from astropy.table import Table
import pkg_resources
import pytest

from .. import models
from ..tasks import ref


@pytest.fixture
def mock_refstable():
    filename = 'data/ztf_ref_table.dat'
    with pkg_resources.resource_stream(__name__, filename) as f:
        return Table.read(f, format='ascii')


@pytest.fixture
def mock_client(monkeypatch, mock_refstable):
    client = Mock(**{
        'search.return_value.to_table.return_value': mock_refstable})
    monkeypatch.setattr('growth.too.tasks.ref.client', client)
    return client


def test_refs(mock_client):
    ref.ztf_references()
    field = models.Field.query.filter_by(telescope='ZTF', field_id=324).one()
    assert field.reference_filter_ids == [1]
    assert field.reference_filter_mags == [21.326900]
