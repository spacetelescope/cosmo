import os
import pytest

from cosmo.monitors.osm_shift_monitors import (
    plot_fuv_osm_shift_cenwaves, compute_segment_diff, FuvOsmShiftMonitor, FuvOsmShift1Monitor, FuvOsmShift2Monitor,
    NuvOsmShiftMonitor, NuvOsmShift1Monitor, NuvOsmShift2Monitor
)
from cosmo.monitors.osm_data_models import OSMShiftDataModel

TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/')
TEST_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cosmoconfig_test.yaml')

# Check to make sure that the test config file is being used. If not, don't run the tests
if os.environ['COSMO_CONFIG'] != TEST_CONFIG:
    raise TypeError('Tests should only be executed with the testing configuration file')

# TODO: Need to define a better test data set that includes everything that we need: Both FUV segments, NUV Data,
#  all acq types etc class


@pytest.mark.xfail(reason='Incomplete test data set causes these to break')
class TestFuvOsmShift1Monitor:

    @classmethod
    def setup_class(cls):
        test_datamodel = OSMShiftDataModel
        test_datamodel.files_source = TEST_DATA
        test_datamodel.cosmo_layout = False

        test_monitor = FuvOsmShift1Monitor
        test_monitor.data_model = test_datamodel
        test_monitor.output = os.path.dirname(os.path.abspath(__file__))
        cls.active_test_monitor = test_monitor()
        cls.csv_output = os.path.join(
            os.path.dirname(cls.active_test_monitor.output), f'{cls.active_test_monitor._filename}-outliers.csv'
        )

    @classmethod
    def teardown_class(cls):
        os.remove(cls.active_test_monitor.output)
        os.remove(cls.csv_output)

    def test_data_initialization(self):
        self.active_test_monitor.initialize_data()

    def test_run_analysis(self):
        self.active_test_monitor.run_analysis()

    def test_plotting(self):
        self.active_test_monitor.plot()

    def test_write_output(self):
        self.active_test_monitor.write_figure()
        assert os.path.exists(self.active_test_monitor.output)

    def test_store_results(self):
        self.active_test_monitor.store_results()
        assert os.path.exists(self.csv_output)
