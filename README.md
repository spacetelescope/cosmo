# COSMO

[![astropy](http://img.shields.io/badge/powered%20by-AstroPy-orange.svg?style=flat)](http://www.astropy.org/)

COSMO (COS MOnitoring) is a backend database that is structured specifically for the Cosmic Origins Spectrogragh (COS) team at Space Telescope Science Institute (STScI). COSMO uses a MySQL database server with SQL Alchemy, an object-relational mapper (ORM) for python. This allows for interfacing with monitors that are important for extending the mission lifetime of COS. The database is broken up into different relational tables which include header keyword and monitoring data. The system is set up to query these tables by the monitors and then returns deliverables such as reference files, figures and CRDS delivery forms. This allows us to have the most up-to-date resources available for the COS team and community.


# Build status
Documentation: [![Documentation Status](https://readthedocs.org/projects/cosmo/badge/?version=latest)](http://cosmo.readthedocs.io/en/latest/?badge=latest)

Unit Tests: [![Build Status](https://travis-ci.org/spacetelescope/cosmo.svg?branch=master)](https://travis-ci.org/justincely/cos_monitoring)
