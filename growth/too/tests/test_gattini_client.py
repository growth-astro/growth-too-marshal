from unittest.mock import MagicMock

from astropy.table import Table
from astropy import time
import pkg_resources
import pytest

from .. import models
from ..tasks import gattini_client


@pytest.fixture
def mock_obstable():
    filename = 'data/gattini_obs_table.dat'
    with pkg_resources.resource_stream(__name__, filename) as f:
        tab = Table.read(f, format='ascii')
        tab["obsjd"] = time.Time(tab["obsjd"], format="jd").datetime
        return tab


@pytest.fixture
def mock_get_obs(monkeypatch, mock_obstable):
    get_obs = MagicMock(**{'return_value': mock_obstable})
    monkeypatch.setattr('growth.too.tasks.gattini_client.get_obs',
                        get_obs)
    return get_obs


def test_obs(mock_get_obs):
    gattini_client.gattini_obs()
    observation = models.Observation.query.filter_by(telescope='Gattini',
                                                     observation_id=84218480,
                                                     subfield_id=0).one()
    assert observation.field_id == 789
    assert observation.limmag == 20.895300
