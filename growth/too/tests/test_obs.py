import os
from astropy.io import ascii

from .. import models
from ..tasks import obs


def test_refs():

    payload = os.path.join(os.path.dirname(os.path.abspath(__file__)),
        'data/ztf_obs_table.dat')

    obstable = ascii.read(payload)
    obs.ztf_obs(obstable=obstable)
    observation = models.Observation.query.filter_by(telescope='ZTF',
                                                     observation_id=84218480,
                                                     subfield_id=54).one()
    assert observation.field_id == 789
    assert observation.limmag == 20.895300
