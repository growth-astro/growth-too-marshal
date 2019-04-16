import os
from urllib.parse import urlparse

import pkg_resources


def mock_download_file(url, *args, **kwargs):
    filename = os.path.join('data', os.path.basename(urlparse(url).path))
    return pkg_resources.resource_filename(__name__, filename)
