import os
import pytest

from glob import glob

from cosmo.monitors.osm_shift_monitors import (
    FuvOsmShift1Monitor, FuvOsmShift2Monitor, NuvOsmShift1Monitor, NuvOsmShift2Monitor
)

from cosmo.monitors.osm_data_models import OSMShiftDataModel


@pytest.fixture(scope='module', autouse=True)
def clean_up(here):
    yield

    output = glob(os.path.join(here, '*html')) + glob(os.path.join(here, '*csv'))

    if output:
        for file in output:
            os.remove(file)


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
