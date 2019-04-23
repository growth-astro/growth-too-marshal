import os
from astropy.io import ascii

from .. import models
from ..tasks import ref


def test_refs():

    payload = os.path.join(os.path.dirname(os.path.abspath(__file__)),
        'data/ztf_ref_table.dat')

    refstable = ascii.read(payload)
    ref.ztf_references(refstable=refstable)
    field = models.Field.query.filter_by(telescope='ZTF', field_id=324).one()
    assert field.reference_filter_ids == [1]
    assert field.reference_filter_mags == [21.326900]
