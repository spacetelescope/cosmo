import os
import pytest

from cosmo.monitors.osm_drift_monitors import FUVOSMDriftMonitor, NUVOSMDriftMonitor

from cosmo.monitors.osm_shift_monitors import (
    FuvOsmShift1Monitor, FuvOsmShift2Monitor, NuvOsmShift1Monitor, NuvOsmShift2Monitor
)

from cosmo.monitors.data_models import OSMDataModel
from cosmo.sms import SMSFinder


@pytest.fixture(params=[False, True])
def set_osmmonitor(request, data_dir, here):
    if request.param:
        model = OSMDataModel()
        model.ingest()

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
    def osmdriftmonitor(self, request, set_osmmonitor):
        osmdriftmonitor = set_osmmonitor(request.param, OSMDataModel)

        request.cls.osmdriftmonitor = osmdriftmonitor

        yield

        if request.cls.osmdriftmonitor.model.model is not None:
            request.cls.osmdriftmonitor.model.model.drop_table(safe=True)

    def test_monitor_steps(self):
        self.osmdriftmonitor.initialize_data()
        self.osmdriftmonitor.run_analysis()
        self.osmdriftmonitor.plot()
        self.osmdriftmonitor.write_figure()
        self.osmdriftmonitor.store_results()

        assert os.path.exists(self.osmdriftmonitor.output)


class TestOsmShiftMonitors:

    @pytest.fixture(
        autouse=True,
        params=[
            FuvOsmShift1Monitor,
            FuvOsmShift2Monitor,
            pytest.param(NuvOsmShift1Monitor, marks=pytest.mark.xfail),
            pytest.param(NuvOsmShift2Monitor, marks=pytest.mark.xfail)
        ]
    )
    def osmshiftmonitor(self, request, set_osmmonitor):
        osmshiftmonitor = set_osmmonitor(request.param, OSMDataModel)

        request.cls.osmshiftmonitor = osmshiftmonitor

        yield

        if request.cls.osmshiftmonitor.model.model is not None:
            request.cls.osmshiftmonitor.model.model.drop_table(safe=True)

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
