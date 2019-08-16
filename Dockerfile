#
# Stage 1: wheelbuilder
# Build some Python packages that do not exist as binaries in apt or PyPI.
#

FROM quay.io/pypa/manylinux1_x86_64 AS wheelbuilder
RUN /opt/python/cp37-cp37m/bin/pip wheel --no-deps \
    lscsoft-glue \
    ligo-segments \
    python-ligo-lw \
    git+https://github.com/mher/flower@1a291b31423faa19450a272c6ef4ef6fe8daa286
RUN for wheel in *.whl; do auditwheel repair $wheel; done
RUN cp *none-any.whl /wheelhouse


#
# Stage 2: aptinstall
# Install as many of our dependencies as possible with apt.
#

FROM debian:testing-slim AS aptinstall

RUN apt-get update && apt-get -y install --no-install-recommends \
    openssh-client \
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
    python3-setuptools \
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

# Debian's pip is too old to install manylinux2010 wheels.
RUN pip3 install --upgrade pip


#
# Stage 3: pipinstall
# Install remaining dependencies with apt.
#

FROM aptinstall AS pipinstall

# Install requirements. Do this before installing our own package, because
# presumably the requirements change less frequently than our own code.
COPY requirements.txt /
COPY --from=wheelbuilder /wheelhouse /wheelhouse
RUN pip3 install --no-cache-dir -f /wheelhouse -r /requirements.txt


#
# Stage 4: (final build)
# Overlay pip dependencies, install our own source, and set configuration.
#

FROM aptinstall
COPY --from=pipinstall /usr/local /usr/local

# Install our own source code now
COPY . /src
RUN pip3 install --no-cache-dir /src

# Set locale (needed for Flask CLI)
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

# Add host fingerprints.
COPY docker/etc/ssh/ssh_known_hosts /etc/ssh/ssh_known_hosts

# Provide SSH keys through Docker secrets.
# Note that SSH correctly guesses the public key by appending ".pub".
RUN echo IdentityFile /run/secrets/id_rsa >> /etc/ssh/ssh_config

# Tell Celery that we don't care about security and that yes,
# please shut up and run as root.
# http://docs.celeryproject.org/en/latest/userguide/daemonizing.html#running-the-worker-with-superuser-privileges-root
ENV C_FORCE_ROOT 1

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
COPY docker/usr/var/growth.too.flask-instance/application.cfg /usr/var/growth.too.flask-instance/application.cfg

# FIXME: generate the Flask secret key here. This should probably be specified
# as an env variable or a docker-compose secret so that it is truly persistent.
# As it is here, it will be regenerated only rarely, if the above steps change.
RUN python3 -c 'import os; print("SECRET_KEY =", os.urandom(24))' \
    >> /usr/var/growth.too.flask-instance/application.cfg

ENTRYPOINT ["growth-too"]
