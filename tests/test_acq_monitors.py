import os
import pytest

from cosmo.monitors.acq_monitors import AcqPeakdMonitor, AcqImageMonitor, AcqPeakxdMonitor
from cosmo.monitors.data_models import AcqDataModel


@pytest.fixture(params=[False, True])
def set_acqmonitor(request, data_dir, here):
    if request.param:
        model = AcqDataModel()
        model.ingest()

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

        yield

        if request.cls.acqmonitor.model.model is not None:
            request.cls.acqmonitor.model.model.drop_table(safe=True)

    def test_monitor_steps(self):
        self.acqmonitor.initialize_data()
        self.acqmonitor.run_analysis()
        self.acqmonitor.plot()
        self.acqmonitor.write_figure()
        self.acqmonitor.store_results()

        assert os.path.exists(self.acqmonitor.output)
