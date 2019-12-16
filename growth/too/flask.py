import datetime
import os

from flask import Flask
from flask_humanize import Humanize
from werkzeug.routing import BaseConverter


# Application object
app = Flask(__name__, instance_relative_config=True)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql:///growth-too-marshal'
app.config['SQLALCHEMY_BINDS'] = {}

app.config['TEMPLATES_AUTO_RELOAD'] = True

# Turn off memory-intensive modification tracking.
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Default secret key: secure and random. However, sessions are not preserved
# across different Python processes.
app.config['SECRET_KEY'] = os.urandom(24)

# Set 16 MB file size upload limit for posting FITS files.
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

# Celery configuration.
# Use pickle serializer, because it supports byte values.
# Use redis broker, because it supports locks (and thus singleton tasks).
app.config['CELERY_BROKER_URL'] = 'redis://'
app.config['CELERY_ACCEPT_CONTENT'] = ['json', 'pickle']
app.config['CELERY_EVENT_SERIALIZER'] = 'json'
app.config['CELERY_RESULT_SERIALIZER'] = 'pickle'
app.config['CELERY_TASK_SERIALIZER'] = 'pickle'

# Email configuration. Put MAIL_PASSWORD in application.cfg.
app.config['EMAIL_TOO'] = 'emgw@lists.astro.caltech.edu'
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USERNAME'] = 'growthtoomarshal'
app.config['MAIL_USE_SSL'] = False
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_DEFAULT_SENDER'] = '{}@gmail.com'.format(
    app.config['MAIL_USERNAME'])

# Apply instance configuration from application.cfg and application.cfg.d/*.
app.config.from_pyfile('application.cfg', silent=True)
dropin_dir = os.path.join(app.instance_path, 'application.cfg.d')
try:
    dropin_files = os.listdir(dropin_dir)
except (FileNotFoundError, NotADirectoryError):
    pass
else:
    for dropin_file in dropin_files:
        app.config.from_pyfile(os.path.join('application.cfg.d', dropin_file))


class DateTimeConverter(BaseConverter):

    def to_python(self, value):
        try:
            return datetime.datetime.strptime(value, '%y%m%d').date()
        except ValueError:
            try:
                datetime.datetime.strptime(value, '%Y-%m-%d').date()
            except ValueError:
                return datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')

    def to_url(self, value):
        return value.isoformat(timespec='seconds')


app.url_map.converters['datetime'] = DateTimeConverter


class DateConverter(BaseConverter):

    def to_python(self, value):
        try:
            return datetime.datetime.strptime(value, '%y%m%d').date()
        except ValueError:
            return datetime.datetime.strptime(value, '%Y-%m-%d').date()

    def to_url(self, value):
        return value.isoformat()


app.url_map.converters['date'] = DateConverter

humanize = Humanize(app)
