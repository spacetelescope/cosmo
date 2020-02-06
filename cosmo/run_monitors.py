import os
import pytest
import shlex

from argparse import ArgumentParser

from . import monitors
from .sms import SMSFinder


def collection() -> dict:
    """Collect Monitor and DataModel classes. Monitors will only be collected if the monitors package structure is
    maintained (i.e. ensuring that new monitors are included in monitors.__init__.py).

    Monitors will only be collected if they adhere to the "*Monitor" naming conventions, and DataModels will only be
    collected if they adhere to the "*DataModel" naming convention. Any class with "Base" in the name will be ignored as
    it is assumed that these are not complete monitors and should not be executed by themselves.
    """
    collection_set = {'monthly': [], 'daily': [], 'all': [], 'datamodels': []}

    for key, value in monitors.__dict__.items():
        if 'Monitor' in key and 'Base' not in key:
            collection_set['all'].append(value)

            if 'run' in value.__dict__:
                collection_set[value.run].append(value)

    # noinspection PyUnresolvedReferences
    for key, value in monitors.data_models.__dict__.items():
        if 'DataModel' in key and 'Base' not in key:
            collection_set['all'].append(value)
            collection_set['datamodels'].append(value)

    return collection_set


COLLECTION = collection()


@pytest.fixture
def monitor():
    """Fixture-factory that creates a monitor instance from the input class."""
    def _monitor(monitor_class):
        active = monitor_class()

        return active

    return _monitor


@pytest.fixture(params=COLLECTION['monthly'])
def monthly_monitor(request, monitor):
    """Parametrized fixture for monitors that should be executed monthly."""
    active = monitor(request.param)

    yield active

    # If run_ingest is not included in the test session, ingest any new data into the databases
    session_names = [item.name for item in request.session.items]  # names of "test" in the session object

    if 'run_ingest' not in session_names:
        active.model.ingest()


@pytest.fixture(params=COLLECTION['daily'])
def daily_monitor(request, monitor):
    """Parametrized fixture for monitors that should be executed daily."""
    active = monitor(request.param)

    yield active

    # If run_ingest is not included in the test session, ingest any new data into the databases
    session_names = [item.name for item in request.session.items]  # names of "test" in the session object

    if 'run_ingest' not in session_names:
        active.model.ingest()


@pytest.fixture
def sms():
    """Fixture for the SMSFinder object."""
    finder = SMSFinder()

    return finder


@pytest.fixture(params=COLLECTION['datamodels'])
def datamodel(request):
    """Parametrized fixture for DataModels for use in ingestion only."""
    active_model = request.param()

    return active_model


class RunIngestion:

    @pytest.mark.ingest
    @pytest.mark.monthly
    def run_sms_ingest(self, sms):
        """Execute SMS file ingestion. Will be executed before the monthly monitors (since the OSM monitors require that
         new SMS files be ingested first. Additionally, this runner is included in the "ingest" group.
         """
        sms.ingest_files()

    @pytest.mark.ingest
    def run_ingest(self, datamodel):
        """Execute DataModel new data discovery and ingestion. Included in the "ingest" group."""
        datamodel.ingest()


class RunMonitors:
    """Class for organizing runners."""

    @pytest.mark.monthly
    def run_monthly(self, monthly_monitor):
        """Execute monitors marked as monthly."""
        monthly_monitor.monitor()


def runner():
    """Function for running the monitors with pytest. Intended as an entry-point for use via the commandline."""
    here = os.path.dirname(os.path.abspath(__file__))

    default_pytest_args = f'{here}'  # Any additional arguments that should be called with pytest.

    # Parse commandline arguments
    parser = ArgumentParser()

    parser.add_argument('--monthly', '-mo', action='store_true', help='Execute Monitors marked as "monthly"')
    parser.add_argument('--ingest', '-in', action='store_true', help='Execute data ingestion for DataModels and SMS')

    args = parser.parse_args()

    # Execute pytest
    if args.monthly:
        pytest.main(shlex.split(default_pytest_args + ' -m monthly'))

        return

    if args.ingestion:
        pytest.main(shlex.split(default_pytest_args + ' -m ingest'))

        return

    pytest.main(shlex.split(default_pytest_args))
