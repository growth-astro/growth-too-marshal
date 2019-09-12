[![Documentation Status](https://readthedocs.org/projects/growth-too-marshal/badge/?version=latest)](https://growth-too-marshal.readthedocs.io/en/latest/?badge=latest)
[![Test Status](https://travis-ci.org/growth-astro/growth-too-marshal.svg?branch=master)](https://travis-ci.org/growth-astro/growth-too-marshal)
[![Docker Cloud Build Status](https://img.shields.io/docker/cloud/build/growthastro/growth-too-marshal)](https://hub.docker.com/r/growthastro/growth-too-marshal)
[![Coverage Status](https://coveralls.io/repos/github/growth-astro/growth-too-marshal/badge.svg?branch=master)](https://coveralls.io/github/growth-astro/growth-too-marshal?branch=master)

# GROWTH Target of Opportunity Marshal

This is the source repository for the GROWTH Target of Opportunity Marshal, or
ToO Marshal for short. It is a platform that has been developed by the Global
Relay of Observatories Watching Transients Happen (GROWTH) collaboration in
order to coordinate follow-up observations of multimessenger transients. The
ToO Marshal's responsibilities include:

1.  Ingest alerts for astrophysical multimessenger transients from LIGO/Virgo,
    IceCube, Fermi, Swift, and other experiments.

2.  Notify on-duty GROWTH astronomers when multimessenger transients occur that
    meet triggering criteria for science programs.

3.  Plan optimal observations for a heterogeneous network of ground-based
    telescopes including ZTF, DECam, KPED, Gattini, and GROWTH-India.

4.  Submit observations to robotic telescope queues and monitor the progress of
    observations.

5.  Provide a central interface for vetting candidates from these facilities in
    combination with external data sources including the Census of the Local
    Universe (CLU) galaxy catalog.

6.  Automatically compose GCN Circular astronomical bulletins.

![Screen shots](https://github.com/growth-astro/growth-too-marshal/raw/master/screenshots.png)

At its core, the ToO Marshal is powered by two open-source Python packages:
[ligo.skymap] for processing and manipulating gravitational-wave localizations,
and [gwemopt] for multi-facility optimal tiling and observation scheduling.

Architecturally, the ToO Marshal is a [Flask] web application backed by a
[PostgreSQL] database and using a [Celery] asynchronous task queue for
supervising long-running background operations.

[ligo.skymap]: https://git.ligo.org/lscsoft/ligo.skymap
[gwemopt]: https://github.com/mcoughlin/gwemopt
[Flask]: http://flask.pocoo.org
[PostgreSQL]: https://www.postgresql.org
[Celery]: http://www.celeryproject.org
