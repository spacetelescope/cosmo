import os
import pytest

from cosmo.monitors.acq_monitors import AcqPeakdMonitor, AcqImageMonitor, AcqPeakxdMonitor
from cosmo.monitors.acq_data_models import AcqPeakdModel, AcqPeakxdModel, AcqImageModel


@pytest.fixture
def set_monitor(data_dir, here):
    def _set_monitor(monitor, datamodel):
        datamodel.files_source = data_dir
        datamodel.cosmo_layout = False

        monitor.data_model = datamodel
        monitor.output = here

        active = monitor()

        return active

    return _set_monitor


class TestAcqMonitors:

    @pytest.fixture(
        autouse=True,
        params=[
            (AcqImageMonitor, AcqImageModel),
            # (AcqImageV2V3Monitor, AcqImageModel),  This set still needs some additional data
            (AcqPeakdMonitor, AcqPeakdModel),
            (AcqPeakxdMonitor, AcqPeakxdModel)
        ]
    )
    def acqmonitor(self, request, set_monitor):
        acqmonitor = set_monitor(*request.param)

        request.cls.acqmonitor = acqmonitor

    def test_monitor_steps(self):
        self.acqmonitor.initialize_data()
        self.acqmonitor.run_analysis()
        self.acqmonitor.plot()
        self.acqmonitor.write_figure()
        self.acqmonitor.store_results()

        assert os.path.exists(self.acqmonitor.output)
