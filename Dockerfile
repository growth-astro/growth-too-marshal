#
# Stage 1: wheel-deps
# Build Python wheels for dependencies that are not in apt or PyPI.
#

FROM quay.io/pypa/manylinux2010_x86_64 AS wheel-deps

RUN /opt/python/cp37-cp37m/bin/pip wheel --no-deps --no-cache-dir \
    lscsoft-glue \
    ligo-segments \
    python-ligo-lw \
    git+https://github.com/mher/flower@1a291b31423faa19450a272c6ef4ef6fe8daa286 && \
    # Audit all binary wheels
    ls *linux_x86_64.whl | xargs -L 1 auditwheel repair && \
    # Copy all architecture-independent wheels
    mv *none-any.whl /wheelhouse && \
    # Clean up to reduce size of cache
    rm *.whl


#
# Stage 2: wheel-self
# Build a Python wheel for this package itself.
#
FROM quay.io/pypa/manylinux2010_x86_64 AS wheel-self
COPY . /src
RUN /opt/python/cp37-cp37m/bin/pip wheel --no-deps --no-cache-dir -w /wheelhouse /src


#
# Stage 3: apt-install
# Install as many of our dependencies as possible with apt.
# Not that we also update pip because Debian's pip is too old to install
# manylinux2010 wheels.
#

FROM debian:stable-slim AS apt-install

RUN apt-get update && apt-get -y install --no-install-recommends \
    ipython3 \
    gunicorn3 \
    openssh-client \
    python3-astropy \
    python3-celery \
    python3-dateutil \
    python3-ephem \
    python3-flask \
    python3-flask-login \
    python3-flask-sqlalchemy \
    python3-future \
    python3-gevent \
    python3-healpy \
    python3-humanize \
    python3-h5py \
    python3-lxml \
    python3-flask-mail \
    python3-freezegun \
    python3-matplotlib \
    python3-networkx \
    python3-numpy \
    python3-pandas \
    python3-passlib \
    python3-phonenumbers \
    python3-pip \
    python3-psycopg2 \
    python3-redis \
    python3-scipy \
    python3-seaborn \
    python3-setuptools \
    python3-socks \
    python3-shapely \
    python3-sqlalchemy-utils \
    python3-tornado \
    python3-twilio \
    python3-tz \
    python3-wtforms && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --upgrade --no-cache-dir pip


#
# Stage 4: pip-install-deps
# Install remaining dependencies with pip.
#

FROM apt-install AS pip-install-deps

# Install requirements. Do this before installing our own package, because
# presumably the requirements change less frequently than our own code.
COPY requirements.txt /
COPY --from=wheel-deps /wheelhouse /wheelhouse
RUN pip3 install --no-cache-dir -f /wheelhouse \
    flower \
    -r /requirements.txt


#
# Stage 5: pip-install-self
# Install our own wheel.
#

FROM apt-install AS pip-install-self
COPY --from=wheel-self /wheelhouse /wheelhouse
RUN pip3 install --no-cache-dir --no-deps /wheelhouse/*.whl


#
# Stage 6: (final build)
# Overlay pip dependencies, install our own source, and set configuration.
#

FROM apt-install
COPY --from=pip-install-deps /usr/local /usr/local
COPY --from=pip-install-self /usr/local /usr/local

# Set locale (needed for Flask CLI)
ENV LC_ALL=C.UTF-8 LANG=C.UTF-8

RUN useradd -mr growth-too-marshal && \
    echo IdentityFile /run/secrets/id_rsa >> /etc/ssh/ssh_config && \
    mkdir -p /usr/var/growth.too.flask-instance && \
    mkdir -p /usr/var/growth.too.flask-instance/catalog && \
    mkdir -p /usr/var/growth.too.flask-instance/input && \
    ln -s /run/secrets/application.cfg.d /usr/var/growth.too.flask-instance/application.cfg.d && \
    ln -s /run/secrets/htpasswd /usr/var/growth.too.flask-instance/htpasswd && \
    ln -s /run/secrets/GROWTH-India.tess /usr/var/growth.too.flask-instance/input/GROWTH-India.tess && \
    ln -s /run/secrets/CLU.hdf5 /usr/var/growth.too.flask-instance/catalog/CLU.hdf5
COPY docker/etc/ssh/ssh_known_hosts /etc/ssh/ssh_known_hosts
COPY docker/usr/var/growth.too.flask-instance/application.cfg /usr/var/growth.too.flask-instance/application.cfg
COPY docker/entrypoint.sh /entrypoint.sh

USER growth-too-marshal:growth-too-marshal
WORKDIR /home/growth-too-marshal

# Prime some cached Astropy data.
RUN growth-too iers

ENTRYPOINT ["/entrypoint.sh"]
