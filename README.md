# COSMO

[![astropy](http://img.shields.io/badge/powered%20by-AstroPy-orange.svg?style=flat)](http://www.astropy.org/)

COSMO (COS MOnitoring) is a backend database that is structured specifically for the Cosmic Origins Spectrogragh (COS) team at Space Telescope Science Institute (STScI). COSMO uses a MySQL database server with SQL Alchemy, an object-relational mapper (ORM) for python. This allows for interfacing with monitors that are important for extending the mission lifetime of COS. The database is broken up into different relational tables which include header keyword and monitoring data. The system is set up to query these tables by the monitors and then returns deliverables such as reference files, figures and CRDS delivery forms. This allows us to have the most up-to-date resources available for the COS team and community.

# Contributing

Please open a new [issue](https://github.com/spacetelescope/cosmo/issues) or new
[pull request](https://github.com/spacetelescope/cosmo/pulls) for
bugs, feedback, or new features you would like to see.

To contribute code, please use the following workflow:

1.  Fork this repository to your personal space
2.  Create a feature branch on your fork for your changes
3.  Make changes and commit into your fork
4.  Issue a pull request to get your changes into the main repo

For more details, the
[astropy workflow](http://docs.astropy.org/en/stable/development/workflow/development_workflow.html)
has more information.

# Build status
Documentation: [![Documentation Status](https://readthedocs.org/projects/cosmo/badge/?version=latest)](http://cosmo.readthedocs.io/en/latest/?badge=latest)

Unit Tests: [![Build Status](https://travis-ci.org/spacetelescope/cosmo.svg?branch=master)](https://travis-ci.org/justincely/cos_monitoring)

Coverage: [![codecov](https://codecov.io/gh/spacetelescope/cosmo/branch/master/graph/badge.svg)](https://codecov.io/gh/TechnionYP5777/project-name)
