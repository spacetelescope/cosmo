import os
import pytest

from glob import glob

from cosmo.monitors.osm_shift_monitors import (
    FuvOsmShift1Monitor, FuvOsmShift2Monitor, NuvOsmShift1Monitor, NuvOsmShift2Monitor
)

from cosmo.monitors.osm_data_models import OSMShiftDataModel

TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/')


@pytest.fixture(scope='module', autouse=True)
def clean_up():
    yield

    path = os.path.dirname(os.path.abspath(__file__))
    output = glob(os.path.join(path, '*html')) + glob(os.path.join(path, '*csv'))

    if output:
        for file in output:
            os.remove(file)


@pytest.fixture
def set_monitor():
    def _set_monitor(monitor, datamodel):
        datamodel.files_source = TEST_DATA
        datamodel.cosmo_layout = False

        monitor.data_model = datamodel
        monitor.output = os.path.dirname(os.path.abspath(__file__))

        active = monitor()

        return active

    return _set_monitor


class TestOsmShiftMonitors:

    @pytest.fixture(
        autouse=True, params=[FuvOsmShift1Monitor, FuvOsmShift2Monitor, NuvOsmShift1Monitor, NuvOsmShift2Monitor]
    )
    def osmshiftmonitor(self, request, set_monitor):
        osmshiftmonitor = set_monitor(request.param, OSMShiftDataModel)

        request.cls.osmshiftmonitor = osmshiftmonitor

    def test_monitor_steps(self):
        self.osmshiftmonitor.initialize_data()
        self.osmshiftmonitor.run_analysis()
        self.osmshiftmonitor.plot()
        self.osmshiftmonitor.write_figure()
        self.osmshiftmonitor.store_results()

        assert os.path.exists(self.osmshiftmonitor.output)

        if 'Fuv' in self.osmshiftmonitor.name:
            assert os.path.exists(
                os.path.join(
                    os.path.dirname(self.osmshiftmonitor.output),
                    f'{self.osmshiftmonitor._filename}-outliers.csv'
                )
            )
