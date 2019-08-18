import os

from astroquery.vizier import VizierClass
from astropy.table import Column, Table
from celery.local import PromiseProxy
import numpy as np
import pkg_resources

from .flask import app


vizier = VizierClass(row_limit=-1)


def fixup(table):
    # Add dummy 2D and 3D credible level columns.
    # These columns are filled with nans because they are
    # localization dependent.
    table.add_column(Column(np.repeat(np.nan, len(table))), 4, '3D CL')
    table.add_column(Column(np.repeat(np.nan, len(table))), 4, '2D CL')

    table = Table(table, masked=True)
    table.convert_bytestring_to_unicode()

    for column in table.columns.values():
        if np.issubsctype(column, np.floating):
            column.format = '%.02f'
            column.mask = np.isnan(column)
        elif np.issubsctype(column, np.unicode):
            column.mask = (column == '')

    table['ra'].format = '%.04f'
    table['dec'].format = '%+.4f'
    return table


def get_from_vizier(*args, **kwargs):
    result, = vizier.query_constraints(*args, **kwargs, cache=True)
    result.convert_bytestring_to_unicode()
    return fixup(result)


def get_from_package(filename):
    filepath = os.path.join('catalog', filename)
    try:
        f = app.open_instance_resource(filepath)
    except IOError:
        f = pkg_resources.resource_stream(__name__, filepath)
    filepath = f.name
    f.close()
    result = Table.read(filepath)
    return fixup(result)


twomass = PromiseProxy(get_from_vizier, ('J/ApJS/199/26/table3',))


galaxies = clu = PromiseProxy(get_from_package, ('CLU.hdf5',))
