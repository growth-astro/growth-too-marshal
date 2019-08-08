# Inherit SSH configuration from Dockerfile.tunnel
FROM tunnel

RUN apt-get update && apt-get -y install \
    git \
    python3-astropy \
    python3-astroquery \
    python3-celery \
    python3-dateutil \
    python3-ephem \
    python3-flask \
    python3-flask-login \
    python3-flask-sqlalchemy \
    python3-future \
    python3-healpy \
    python3-humanize \
    python3-h5py \
    python3-lxml \
    python3-flask-mail \
    python3-matplotlib \
    python3-networkx \
    python3-numpy \
    python3-pandas \
    python3-passlib \
    python3-phonenumbers \
    python3-pip \
    python3-psycopg2 \
    python3-redis \
    python3-reproject \
    python3-scipy \
    python3-seaborn \
    python3-socks \
    python3-shapely \
    python3-sqlalchemy-utils \
    python3-tqdm \
    python3-tornado \
    python3-twilio \
    python3-tz \
    python3-wtforms \
    python3-pyvo && \
    rm -rf /var/lib/apt/lists/*

# Set locale (needed for Flask CLI)
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

# Tell Celery that we don't care about security and that yes,
# please shut up and run as root.
# http://docs.celeryproject.org/en/latest/userguide/daemonizing.html#running-the-worker-with-superuser-privileges-root
ENV C_FORCE_ROOT 1

# Debian's pip is too old to install manylinux2010 wheels.
RUN pip3 install --upgrade pip

# Install requirements. Do this before installing our own package, because
# presumably the requirements change less frequently than our own code.
COPY requirements.txt /
RUN pip3 install --no-cache-dir -r \
    /requirements.txt \
    git+https://github.com/mher/flower@1a291b31423faa19450a272c6ef4ef6fe8daa286
RUN rm /requirements.txt

# Prime some cached Astropy data sources.
RUN python3 -c 'from astropy.coordinates import EarthLocation; from astroplan import download_IERS_A; EarthLocation.get_site_names(); download_IERS_A()'

RUN mkdir -p /usr/var/growth.too.flask-instance && \
    mkdir -p /usr/var/growth.too.flask-instance/too/catalog && \
    mkdir -p /usr/var/growth.too.flask-instance/input && \
    ln -s /run/secrets/application.cfg.d /usr/var/growth.too.flask-instance/application.cfg.d && \
    ln -s /run/secrets/htpasswd /usr/var/growth.too.flask-instance/htpasswd && \
    ln -s /run/secrets/netrc /root/netrc && \
    ln -s /run/secrets/input/GROWTH-India.tess /usr/var/growth.too.flask-instance/input/GROWTH-India.tess && \
    ln -s /run/secrets/too/catalog/CLU.hdf5 /usr/var/growth.too.flask-instance/too/catalog/CLU.hdf5
COPY docker/application.cfg /usr/var/growth.too.flask-instance/application.cfg

# FIXME: generate the Flask secret key here. This should probably be specified
# as an env variable or a docker-compose secret so that it is truly persistent.
# As it is here, it will be regenerated only rarely, if the above steps change.
RUN python3 -c 'import os; print("SECRET_KEY =", os.urandom(24))' \
    >> /usr/var/growth.too.flask-instance/application.cfg

COPY . /src
RUN pip3 install --no-cache-dir /src

ENTRYPOINT ["growth-too"]
