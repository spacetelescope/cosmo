import os
import pytest

from cosmo.monitors.acq_monitors import AcqPeakdMonitor, AcqImageMonitor, AcqPeakxdMonitor
from cosmo.monitors.data_models import AcqDataModel

# TODO: Add tests to make sure that the monitors can work with or without db


@pytest.fixture
def set_acqmonitor(data_dir, here):
    def _set_monitor(monitor):
        AcqDataModel.files_source = data_dir
        AcqDataModel.cosmo_layout = False

        monitor.data_model = AcqDataModel
        monitor.output = here

        active = monitor()

        return active

    return _set_monitor


class TestAcqMonitors:

    @pytest.fixture(autouse=True, params=[AcqImageMonitor, AcqPeakdMonitor, AcqPeakxdMonitor])
    def acqmonitor(self, request, set_acqmonitor):
        acqmonitor = set_acqmonitor(request.param)

        request.cls.acqmonitor = acqmonitor

    def test_monitor_steps(self):
        self.acqmonitor.initialize_data()
        self.acqmonitor.run_analysis()
        self.acqmonitor.plot()
        self.acqmonitor.write_figure()
        self.acqmonitor.store_results()

        assert os.path.exists(self.acqmonitor.output)
