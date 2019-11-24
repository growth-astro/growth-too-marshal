from unittest.mock import MagicMock

import numpy as np
import pkg_resources
import pytest

from .. import models
from ..tasks import decam_client


@pytest.fixture
def mock_obstable():
    filename = 'data/decam_obs_table.dat'
    with pkg_resources.resource_stream(__name__, filename) as f:
        tab = np.loadtxt(f)
        return tab


@pytest.fixture
def mock_get_obs(monkeypatch, mock_obstable):
    get_obs = MagicMock(**{'return_value': mock_obstable})
    monkeypatch.setattr('growth.too.tasks.decam_client.get_obs',
                        get_obs)
    return get_obs


def test_obs(mock_get_obs):
    decam_client.decam_obs()
    observation = models.Observation.query.filter_by(telescope='DECam',
                                                     field_id=11543,
                                                     subfield_id=17).one()
    assert observation.field_id == 11543
    assert observation.limmag == 24.40897
