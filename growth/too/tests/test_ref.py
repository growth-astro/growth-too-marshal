import astropy

from .. import models
from ..tasks import ref


def test_refs():
    data_rows = [(1, 2, 1, 1, 22.0),
                 (1, 2, 1, 2, 23.0),
                 (2, 2, 1, 2, 23.0)]
    names = ('field', 'ccid', 'qid', 'fid', 'maglimit')
    refstable = astropy.table.Table(rows=data_rows, names=names)
    ref.ztf_references(refstable=refstable)
    field = models.Field.query.filter_by(telescope='ZTF', field_id=1).one()
    assert field.reference_filter_ids == [1, 2]
    assert field.reference_filter_mags == [22.0, 23.0]
