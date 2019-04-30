.. highlight:: shell-session

Installation
============

Supported Python versions
-------------------------

The growth-too-marshal project requires Python 3.6.

Install using Conda for development and testing
-----------------------------------------------

These instructions use the `Miniconda`_ Python distribution and are suitable
for installing growth-too-marshal for development and testing on any Linux or
macOS machine. If you already have Miniconda or Anaconda installed, then skip
the first two steps.

1.  Download the 64-bit Python 3 installer for `Miniconda`_ for your operating
    system.

    *   If you are on Linux, run this command::

            $ curl https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh > miniconda.sh

    *   If you are on macOS, run this command::

            $ curl https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.sh > miniconda.sh

2.  Run the Miniconda installer::

        $ sh miniconda.sh

    Agree to the terms and conditions and install to the directory of your
    choice. No need to run `sudo` here if you are installing for local
    development. By default, it will install into ``~/miniconda3``, which is
    just fine.

    When the installer asks, ``Do you wish the installer to initialize
    Miniconda3 in your ~/.bash_profile ? [yes|no]``, I suggest answering
    `no`.

    ..  note::

        For unattended, non-interactive installation, you can add the `-b`
        option to automatically agree to the license terms::

            $ sh miniconda.sh -bf

3.  Create a new Conda environment with this command::

        $ ~/miniconda3/bin/conda create -ym --prefix=~/growth-too-marshal python=3.6

4.  "Activate" the environment to add it to your current shell session::

        $ source ~/miniconda3/bin/activate ~/growth-too-marshal

5.  Next, we will install several pre-built Python packages using ``conda``
    itself:

        $ conda config --add channels anaconda
        $ conda config --add channels conda-forge
        $ conda install -y astropy astropy-healpix celery ephem flask flask-login flask-mail flask-sqlalchemy flask-wtf flower healpy humanize h5py ipython ligo-gracedb ligo-segments ligo.skymap lxml networkx pandas passlib postgresql psycopg2 pygcn pytest pytz pyvo redis redis-py sphinx sqlalchemy sqlalchemy-utils

6.  Next, we'll check out the source code with ``git``::

        $ git clone https://github.com/growth-astro/growth-too-marshal.git ~/growth-too-marshal/src

7.  Install the marshal itself, and its remaining dependencies, using ``pip``::

        $ pip install -e ~/growth-too-marshal/src

The ToO Marshal is now installed. Optionally, you can run the unit tests at
this point to check that everything was installed correctly::

    $ cd ~/growth-too-marshal/src
    $ python setup.py test

Now, proceed to the next section to configure the PostgreSQL database.

Configure PostgreSQL
~~~~~~~~~~~~~~~~~~~~

The ToO Marshal uses a `PostgreSQL`_ database to store all of its data. Follow
these instructions to initialize, start, and populate the PostgreSQL database.

..  note::

    These instructions are suitable for using the Conda installation of
    PostgreSQL. Advanced users might want to adapt these instructions to their
    own needs by using a PostgreSQL database that is installed and managed by
    their package manager such as ``apt-get`` or ``port``.

1.  Initialize PostgreSQL by running this command::

    $ initdb -D ~/growth-too-marshal/var/lib/postgresql

2.  Start the PostgreSQL server::

    $ pg_ctl -D ~/growth-too-marshal/var/lib/postgresql start

3.  Create an empty database for the ToO Marshal::

    $ createdb growth-too-marshal

4.  The ToO Marshal provides a tool to create and populate its tables.

    *   (Recommended for development) To create the tables and populate them
        with some sample events and a sample user account::

        $ growth-too db create --sample

    *   Or, to create the tables without any sample events or user accounts::

        $ growth-too db create

The PostgreSQL database is now initialized, running, and populated. Proceed to
the next section to start Redis.

Configure Redis
~~~~~~~~~~~~~~~

The ToO Marshal uses `Redis`_ as a backend for its `Celery`_ asynchronous task
queue for managing background jobs. To start Redis, run this command::

    $ redis-server --daemonize yes

The Redis server is now running. Proceed to the next section for application
configuration.

Application configuration for development
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are a few last steps to complete the configuration of the ToO Marshal for
development and testing.

1.  The GROWTH ToO Marshal fetches user passwords from an `htpasswd`_ file.
    Create an htpasswd file with a password for the sample user ``fritz`` (as
    in `Fritz Zwicky`_, of course) by running this command and entering a
    password::

        $ growth-too passwd fritz

.. _`requirements.txt`: https://github.com/growth-astro/growth-too-marshal/blob/master/requirements.txt
.. _`pip`: https://pip.pypa.io/en/stable/
.. _`Miniconda`: https://docs.conda.io/en/latest/miniconda.html
.. _`PostgreSQL`: https://www.postgresql.org
.. _`Redis`: https://redis.io
.. _`Celery`: http://www.celeryproject.org
.. _`htpasswd`: https://httpd.apache.org/docs/2.4/programs/htpasswd.html
.. _`Fritz Zwicky`: https://en.wikipedia.org/wiki/Fritz_Zwicky
