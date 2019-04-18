"""SQLAlchemy ORM classes for GROWTH Marshal database, with columns discovered
by reflection."""
import warnings

from .flask import app
from . import models

from sqlalchemy import Table


class Users(models.db.Model):
    __bind_key__ = 'growthdb'
    __table__ = Table(
        'users', models.db.metadata, autoload=True,
        autoload_with=models.db.get_engine(app, bind=__bind_key__))


class SciencePrograms(models.db.Model):
    __bind_key__ = 'growthdb'
    __table__ = Table(
        'scienceprograms', models.db.metadata, autoload=True,
        autoload_with=models.db.get_engine(app, bind=__bind_key__))


def get_marshallink(username, program_name):

    user = models.db.session.query(Users).filter_by(username=username).all()
    if len(user) == 0 or user is None:
        warnings.warn('User missing from growth-db')
        return 'None'
    else:
        programid_string = user[0].group_memberships

    program = models.db.session.query(
        SciencePrograms).filter_by(name=program_name).all()
    if len(program) == 0 or program is None:
        warnings.warn('Science program missing from growth-db')
        return 'None'
    else:
        programid = str(program[0].id)

    try:
        cutprogramidx = programid_string.split().index(programid)
        baselink = 'http://skipper.caltech.edu:8080/cgi-bin/growth/growth_treasures_transient.cgi?cutprogramidx=%d' % cutprogramidx  # noqa: E501
    except ValueError:
        cutprogramidx = -1
        baselink = 'None'

    return baselink
