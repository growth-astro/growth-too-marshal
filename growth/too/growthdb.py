"""SQLAlchemy ORM classes for GROWTH Marshal database, with columns discovered
by reflection."""

from .flask import app
from .models import db

from sqlalchemy import Table


class Users(db.Model):
    __bind_key__ = 'growthdb'
    __table__ = Table(
        'users', db.metadata, autoload=True,
        autoload_with=db.get_engine(app, bind=__bind_key__))


class SciencePrograms(db.Model):
    __bind_key__ = 'growthdb'
    __table__ = Table('scienceprograms', db.metadata, autoload=True,
    autoload_with=db.get_engine(app, bind=__bind_key__))


def get_marshallink(username, program_name):

    user = db.session.query(Users).filter_by(username=username).all()
    if len(user) == 0 or user is None:
        warnings.warn('User missing from growth-db')
        return 'None'
    else:
        programid_string = user[0].group_memberships

    program = session.query(SciencePrograms).filter_by(name=program_name).all()
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
