from ..tasks import growthdb_cgi
from .. import models


def test_update_candidates(httpserver, monkeypatch, celery):
    monkeypatch.setattr(growthdb_cgi, 'BASE_URL', httpserver.url_for('/'))

    httpserver.expect_oneshot_request(
        '/list_programs.cgi', method='GET'
    ).respond_with_json(
        [{'programidx': 0,
          'name': 'Afterglows of Fermi Gamma Ray Bursts'},
         {'programidx': 1,
          'name': 'DECAM GW Followup'},
         {'programidx': 2,
          'name': 'Electromagnetic Counterparts to Neutrinos'},
         {'programidx': 3,
          'name': 'Electromagnetic Counterparts to Gravitational Waves'}]
    )

    httpserver.expect_oneshot_request(
        '/list_program_sources.cgi',
        method='GET', query_string={'programidx': '0'}
    ).respond_with_json([{
        'mag_obsdate': '2019-11-17',
        'rcid': 56,
        'creationdate': '2018-04-15',
        'name': 'ZTF18mgpghtk',
        'classification': 'TDE',
        'redshift': '0.051',
        'iauname': 'AT2018ufl',
        'release_status': 'pending',
        'field': 787,
        'candid': 444342588776489533,
        'ra': 176.02946873598754,
        'mag': ['g', 18.93162291904686],
        'lastmodified': '2019-05-01',
        'release_auth': None,
        'dec': -30.22812438404631,
        'last_updated': '2019-11-17 15:21:33',
        'id': 3600
    }])

    httpserver.expect_oneshot_request(
        '/list_program_sources.cgi',
        method='GET', query_string={'programidx': '1'}
    ).respond_with_json([])

    httpserver.expect_oneshot_request(
        '/list_program_sources.cgi',
        method='GET', query_string={'programidx': '2'}
    ).respond_with_json([{
        'mag_obsdate': '2018-06-15',
        'rcid': 5,
        'creationdate': '2018-05-24',
        'name': 'ZTF18netvlyz',
        'classification': None,
        'redshift': None,
        'iauname': '',
        'release_status': 'pending',
        'field': 1060,
        'candid': 972181947620594766,
        'ra': 282.25628798647114,
        'mag': ['r', 20.355316780139297],
        'lastmodified': '2018-05-24',
        'release_auth': None,
        'dec': -48.134372914826464,
        'last_updated': '2019-11-17 16:24:11',
        'id': 9846
    }])

    httpserver.expect_oneshot_request(
        '/list_program_sources.cgi',
        method='GET', query_string={'programidx': '3'}
    ).respond_with_json([])

    httpserver.expect_oneshot_request(
        '/source_summary.cgi',
        method='GET', query_string={'sourceid': '3600'}
    ).respond_with_json({
        'autoannotations': [
            {
                'username': 'foo',
                'datatype': 'STRING',
                'comment': 'Bar!'
            }
        ],
        'uploaded_photometry': [
            {
                'observer': None,
                'sigmaflux': None,
                'ranr': 236.0300398,
                'limmag': 21.2875,
                'sigmamagpsf': 0.192568,
                'sigmamagap': 99.0,
                'obsdate': '2018-07-16 05:04:43',
                'uploadedby': None,
                'instrument': 'ZTF',
                'ra': 236.0295344,
                'rb': None,
                'perinc': None,
                'issub': True,
                'distnr': 0.908012986183,
                'magnr': 17.6940002441,
                'lastmodified': '2018-07-16 15:15:29',
                'deltajd': 0.0,
                'refsys': None,
                'reducedby': None,
                'jd': 2458315.71161,
                'isdiffpos': True,
                'exptime': 30.0,
                'decnr': 68.6009563,
                'magap': 99.0,
                'filter': 'g',
                'flux': None,
                'programid': 2,
                'magpsf': 21.0462,
                'dec': 68.6011396
            }
        ]
    })

    httpserver.expect_oneshot_request(
        '/source_summary.cgi',
        method='GET', query_string={'sourceid': '9846'}
    ).respond_with_json({
        'autoannotations': [
            {
                'username': 'bat',
                'datatype': 'STRING',
                'comment': 'Baz!'
            }
        ],
        'uploaded_photometry': []
    })

    growthdb_cgi.update_candidates()

    assert models.Candidate.query.get('ZTF18mgpghtk').iauname == 'AT2018ufl'
    assert models.Candidate.query.get('ZTF18netvlyz').iauname == ''
