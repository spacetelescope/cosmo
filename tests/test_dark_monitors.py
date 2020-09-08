import os
import pytest

from cosmo.monitors.dark_monitors import DarkMonitor, FUVADarkMonitor, \
    FUVBDarkMonitor, NUVDarkMonitor
from cosmo.monitors.data_models import DarkDataModel


@pytest.fixture(params=[False, True])
def set_darkmonitor(request, data_dir, here):
    if request.param:
        model = DarkDataModel()
        model.ingest()

    def _set_monitor(monitor):
        DarkDataModel.files_source = data_dir
        DarkDataModel.subdir_pattern = None

        monitor.data_model = DarkDataModel
        monitor.output = here

        active = monitor()

        return active

    return _set_monitor


class TestDarkMonitors:

    @pytest.fixture(autouse=True, params=[FUVADarkMonitor,
                                          FUVBDarkMonitor,
                                          NUVDarkMonitor])
    def darkmonitor(self, request, set_darkmonitor):
        darkmonitor = set_darkmonitor(request.param)

        request.cls.darkmonitor = darkmonitor

        yield

        if request.cls.darkmonitor.model.model is not None:
            request.cls.darkmonitor.model.model.drop_table(safe=True)

    def test_monitor_steps(self):
        self.darkmonitor.initialize_data()
        self.darkmonitor.run_analysis()
        self.darkmonitor.plot()
        self.darkmonitor.write_figure()
        self.darkmonitor.store_results()

        assert os.path.exists(self.darkmonitor.output)
