from unittest import mock

import lxml.etree
import pkg_resources

from .. import models
from ..gcn import handle


@mock.patch('growth.too.tasks.skymaps.download.run')
@mock.patch('growth.too.tasks.skymaps.contour.run')
@mock.patch('growth.too.tasks.tiles.tile.run')
def test_lvc(mock_tile, mock_contour,
             mock_download, celery, database, flask, mail):
    """Very basic test of LIGO/Virgo GCN parsing."""
    # Read test GCN
    payload = pkg_resources.resource_string(
        __name__, 'data/MS181101ab-1-Preliminary.xml')
    root = lxml.etree.fromstring(payload)

    # Run function under test
    handle(payload, root)

    dateobs = '2018-11-01T22:22:47'
    event = models.Event.query.get(dateobs)
    assert event.tags == ['LVC', 'GW', 'BNS', 'MDC']
