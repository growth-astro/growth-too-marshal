GROWTH Target of Opportunity Marshal
====================================

.. image:: https://readthedocs.org/projects/growth-too-marshal/badge/?version=latest
   :target: https://growth-too-marshal.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

.. image:: https://travis-ci.org/growth-astro/growth-too-marshal.svg?branch=master
   :target: https://travis-ci.org/growth-astro/growth-too-marshal
   :alt: Test Status

.. image:: https://coveralls.io/repos/github/growth-astro/growth-too-marshal/badge.svg?branch=master
   :target: https://coveralls.io/github/growth-astro/growth-too-marshal?branch=master
   :alt: Coverage Status

This is the source repository for the GROWTH Target of Opportunity Marshal, or
ToO Marshal for short. It is a platform that has been developed by the Global
Relay of Observatories Watching Transients Happen (GROWTH) collaboration in
order to coordinate follow-up observations of multimessenger transients. The
ToO Marshal's responsibilities include:

1. Ingest alerts for astrophysical multimessenger transients from LIGO/Virgo,
   IceCube, Fermi, Swift, and other experiments.

2. Notify on-duty GROWTH astronomers when multimessenger transients occur that
   meet triggering criteria for science programs.

3. Plan optimal observations for a heterogeneous network of ground-based
   telescopes including ZTF, DECam, KPED, Gattini, and GROWTH-India.

4. Submit observations to robotic telescope queues and monitor the progress of
   observations.

5. Provide a central interface for vetting candidates from these facilities in
   combination with external data sources including the Census of the Local
   Universe (CLU) galaxy catalog.

6. Automatically compose GCN Circular astronomical bulletins.

At its core, the ToO Marshal is powered by two open-source Python packages:
`ligo.skymap`_ for processing and manipulating gravitational-wave
localizations, and `gwemopt`_ for multi-facility optimal tiling and observation
scheduling.

Architecturally, the ToO Marshal is a `Flask`_ web application backed by a
`PostgreSQL`_ database and using a `Celery`_ asynchronous task queue for
supervising long-running background operations.

.. _`ligo.skymap`: https://git.ligo.org/lscsoft/ligo.skymap
.. _`gwemopt`: https://github.com/mcoughlin/gwemopt
.. _`Flask`: http://flask.pocoo.org
.. _`PostgreSQL`: https://www.postgresql.org
.. _`Celery`: http://www.celeryproject.org
