Getting Started
===============
COSMO is intended to be installed and its monitors run either manually or via cronjob.
Use of COSMO outside of the scope of executing the monitors that are defined is not recommended, however, the data
models and their API can be used to access the monitoring data for use outside the scope of this project if needed.

Installing
----------
Before installing COSMO, be sure to create an environment with python 3.7+ at minimum.
A good starting point would be::

    conda create -n cosmo_env python=3.7 stsci

For developers, also including ``coverage`` is also recommended (but not mandatory).

After an environment has been prepared, clone the repository::

    git clone https://github.com/spacetelescope/cosmo.git

Then install using pip::

    cd cosmo
    pip install -e .

The ``-e`` argument is required for users who will also be developing and execute tests.

Configuration
--------------
COSMO Settings with a ``monitorframe`` Configuration File and Environment Variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To manage configurations, COSMO primarily uses environment variables on top of the ``monitorframe`` configuration file.

Use the following, minimum set of environment variables to configure COSMO:

.. code-block::

    COSMO_FILES_SOURCE='path/to/data/files'
    COSMO_OUTPUT='path/to/monitor/output'
    COSMO_SMS_SOURCE='path/to/sms/files'

Additional environment variables are available for further configuring the SMS database and are described in the
:ref:`sms-database` section.

``monitorframe`` requires a ``yaml`` configuration file with the following:

.. code-block:: yaml

    # Monitor data database
    data:
      db_settings:
        database: ''
        pragmas:
          journal_mode: 'wal'
          foreign_keys: 1
          ignore_check_constraints: 0
          synchronous: 0

    # Monitor status and results database
    results:
      db_settings:
        database: ''
        pragmas:
          journal_mode: 'wal'
          foreign_keys: 1
          ignore_check_constraints: 0
          synchronous: 0

For more information on sqlite pragma statements, see `this <https://www.sqlite.org/pragma.html>`_.

This configuration file should be set to an environment variable called ``MONITOR_CONFIG``.

.. warning::

    Use proper precautions around your configuration file.
    It may or may not contain sensitive information, so please ensure that permissions on that file are restricted to
    the intended users.
    DON'T push it to GitHub!

CRDS
^^^^
Some of the COSMO DataModels utilize data from reference files, and take advantage of ``crds`` to do so.
For configuration and setup instructions for using ``crds``, see
`the crds user manual <https://hst-crds.stsci.edu/static/users_guide/environment.html>`_.

At minimum, users will need access to a CRDS cache with the following reference file types:

- LAMPTAB
- WCPTAB

Since the COSMO monitors use data from reference files across time, it would be best to get all files of those types
available in the *active context*.

The easiest way to ensure that the local CRDS cache has everything required, users can use::

    crds sync --contexts hst-cos-operational --fetch-references

This command with download *all* COS reference files and mappings to the ``CRDS_CACHE`` (see the instructions mentioned
above).

.. warning::

    The command given above works well, but there's a caveat: it requires a large amount of available storage space at
    the cache location (between 2-3 GB).

Running Tests
-------------
COSMO includes a suite of tests for the package.
For developers, it's a good idea to execute these tests whenever there are changes to the code or environment.

Before executing tests, set the ``MONITOR_CONFIG`` environment variable to the test configuration
that's included with the repository: ``cosmo/tests/cosmoconfig_tests.yaml``, and set the ``COSMO_SMS_DB`` environment
variable to `'test.db'`.

.. note::

    If tests are executed before setting the ``MONITOR_CONFIG`` and ``COSMO_SMS_DB`` environment variables to the test
    configurations, the tests *will not execute*.

If you're in the project directory, you can execute the tests with::

    python -m pytest

For executing the tests with coverage (after ``coverage`` has been installed), use::

    coverage run -m pytest

Executing Monitors
------------------
Monitors can be executed by using the monitoring classes directly:

.. code-block:: python

    from cosmo.monitors import AcqImageMonitor

    monitor = AcqImageMonitor()

    # Run it
    monitor.monitor()

Or, they can be executed from the command line::

    (cosmoenv) mycomputer:~ user$ cosmo --monthly

For more command line options::

    (cosmoenv) mycomputer:~ user$ cosmo --help

