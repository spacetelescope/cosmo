COSMO (COS Monitoring)
==============================
Welcome! This is the documentation for COS Monitoring (COSMO).
This document aims to give a high level overview of COSMO, including it's monitors, the back end, and how it all ties
together.
You can get your copy here: `COSMO <https://github.com/spacetelescope/cosmo>`_

What is COSMO?
---------------
COSMO is a software package of monitors and their data for the Cosmic Origins Spectrogragh (COS) at Space Telescope
Science Institute (STScI).
These monitors are important for ensuring that COS is operating nominally as well as extending its mission lifetime.

COSMO is built on `monitorframe <https://github.com/spacetelescope/monitor-framework>`_, a light-weight framework for
creating instrument monitors (for more information about ``monitorframe``, check out the
`documentation <https://spacetelescope.github.io/monitor-framework/?>`_).

COSMO
-----
.. toctree::
   :maxdepth: 2

   getting_started
   monitors
   data_models
   utilities
   retrieval
   api


Indices and tables
==================
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
