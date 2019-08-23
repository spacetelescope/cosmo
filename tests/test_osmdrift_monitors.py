import os
import pytest

from glob import glob

from cosmo.monitors.osm_drift_monitors import FUVOSMDriftMonitor, NUVOSMDriftMonitor
from cosmo.monitors.osm_data_models import OSMDriftDataModel
from cosmo.sms import SMSFinder


@pytest.fixture(scope='module', autouse=True)
def clean_up(here):
    yield

    output = glob(os.path.join(here, '*html'))

    if output:
        for file in output:
            os.remove(file)


@pytest.fixture
def set_monitor(data_dir, here):
    def _set_monitor(monitor, datamodel):
        # OSM Drift model requires that an SMS database exist
        finder = SMSFinder(data_dir)
        finder.ingest_files()

        datamodel.files_source = data_dir
        datamodel.cosmo_layout = False

        monitor.data_model = datamodel
        monitor.output = here

        active = monitor()

        return active

    return _set_monitor


class TestOSMDriftMonitors:

    @pytest.fixture(autouse=True, params=[FUVOSMDriftMonitor, NUVOSMDriftMonitor])
    def osmdriftmonitor(self, request, set_monitor):
        osmdriftmonitor = set_monitor(request.param, OSMDriftDataModel)

        request.cls.osmdriftmonitor = osmdriftmonitor

    def test_monitor_steps(self):
        self.osmdriftmonitor.initialize_data()
        self.osmdriftmonitor.run_analysis()
        self.osmdriftmonitor.plot()
        self.osmdriftmonitor.write_figure()
        self.osmdriftmonitor.store_results()

        assert os.path.exists(self.osmdriftmonitor.output)
