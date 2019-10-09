COSMO (COS Monitoring)
==============================
Welcome! This is the documentation for COS Monitoring (COSMO).
This document aims to give a high level overview of COSMO, including it's monitors, the back end, and how it all ties
together.

What is COSMO?
--------------
COSMO is a software package of monitors and interfaces for their data for the Cosmic Origins Spectrogragh (COS) at Space
Telescope Science Institute (STScI).
These monitors are important for ensuring that COS is operating nominally as well as extending its mission lifetime.

COSMO is built on `monitorframe <https://github.com/spacetelescope/monitor-framework>`_, a light-weight framework for
creating instrument monitors (for more information about ``monitorframe``, check out the
`documentation <https://spacetelescope.github.io/monitor-framework/?>`_).

The remote repository for COSMO is hosted on `GitHub <https://github.com/spacetelescope/cosmo>`_ and utilizes
`Travis CI <https://travis-ci.org/spacetelescope/cosmo>`_ for its automated testing needs.
COSMO also tracks testing coverage with `Codecov <https://codecov.io/gh/spacetelescope/cosmo>`_.

COSMO
-----
.. toctree::
   :maxdepth: 2

   getting_started
   monitors
   datamodels
   sms
   api


Indices and tables
==================
* :ref:`genindex`