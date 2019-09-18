from unittest import mock
from urllib.error import HTTPError

import lxml.etree
import pkg_resources

import pytest

from .. import models
from ..gcn import handle


@pytest.mark.enable_socket
@mock.patch('urllib.request.urlopen',
            side_effect=HTTPError('some/invalid/url', 404, 'Not found',
                                  'some header', 'some fp'))
@mock.patch('growth.too.tasks.skymaps.download.run')
@mock.patch('growth.too.tasks.skymaps.contour.run')
@mock.patch('growth.too.tasks.tiles.tile.run')
def test_lvc_flatten_map(mock_tile, mock_contour, mock_download,
                         mock_http_response, celery, flask, mail):
    """Very basic test of LIGO/Virgo GCN parsing."""
    # Read test GCN
    payload = pkg_resources.resource_string(
        __name__, 'data/MS181101ab-1-Preliminary.xml')
    root = lxml.etree.fromstring(payload)
    # Run function under test
    handle(payload, root)
    dateobs = '2018-11-01T22:22:47'
    event = models.Event.query.get(dateobs)
    assert mock_download.assert_called_once_with(
            "https://emfollow.docs.ligo.org/userguide/_static/bayestar.fits.gz",  # noqa: E501
        dateobs
    )
    assert event.tags == ['LVC', 'GW', 'BNS', 'MDC']


@pytest.mark.enable_socket
@mock.patch('urllib.request.urlopen')
@mock.patch('growth.too.tasks.skymaps.download.run')
@mock.patch('growth.too.tasks.skymaps.contour.run')
@mock.patch('growth.too.tasks.tiles.tile.run')
def test_lvc_multires_map(mock_tile, mock_contour, mock_download,
                          mock_http_response, celery, flask, mail):
    """Very basic test of LIGO/Virgo GCN parsing."""
    # Read test GCN
    payload = pkg_resources.resource_string(
        __name__, 'data/MS181101ab-1-Preliminary.xml')
    root = lxml.etree.fromstring(payload)
    # Run function under test
    handle(payload, root)
    dateobs = '2018-11-01T22:22:47'
    event = models.Event.query.get(dateobs)
    assert mock_download.assert_called_once_with(
        "https://emfollow.docs.ligo.org/userguide/_static/bayestar.multiorder.fits",  # noqa: E501
        dateobs
    )
    assert event.tags == ['LVC', 'GW', 'BNS', 'MDC']
