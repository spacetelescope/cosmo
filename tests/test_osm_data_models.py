import os
import pandas as pd
import numpy as np

from cosmo.monitors.osm_data_models import OSMShiftDataModel


TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/')


class TestOSMDataModel:

    @classmethod
    def setup_class(cls):
        cls.test_osmdatamodel = OSMShiftDataModel
        cls.test_osmdatamodel.files_source = TEST_DATA
        cls.test_osmdatamodel.cosmo_layout = False

        cls.executed_model = cls.test_osmdatamodel()

    def test_data_collection(self):
        assert isinstance(self.executed_model.data, pd.DataFrame)
        assert len(self.executed_model.data) == 4

    def test_content_collected(self):
        keys_that_should_be_there = (
            # Header keywords
            'ROOTNAME', 'EXPSTART', 'DETECTOR', 'LIFE_ADJ', 'OPT_ELEM', 'CENWAVE', 'FPPOS', 'PROPOSID', 'OBSET_ID',

            # Data extension keywords
            'TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT'
        )

        for key in keys_that_should_be_there:
            assert key in self.executed_model.data

    def test_data_extension_data(self):
        data_extension_keys = ('TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT')

        for key in data_extension_keys:
            assert isinstance(self.executed_model.data[key].values, np.ndarray)
