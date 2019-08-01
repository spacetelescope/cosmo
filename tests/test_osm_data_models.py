import pandas as pd
import numpy as np
import os
import pytest

from cosmo.monitors.osm_data_models import OSMShiftDataModel

TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/')


@pytest.fixture
def test_datamodel():
    model = OSMShiftDataModel
    model.files_source = TEST_DATA
    model.cosmo_layout = False
    test_datamodel = model()

    return test_datamodel


class TestOSMDataModel:

    def test_data_collection(self, test_datamodel):
        assert isinstance(test_datamodel.new_data, pd.DataFrame)
        assert len(test_datamodel.new_data) == 4

    def test_content_collected(self, test_datamodel):
        keys_that_should_be_there = (
            # Header keywords
            'ROOTNAME', 'EXPSTART', 'DETECTOR', 'LIFE_ADJ', 'OPT_ELEM', 'CENWAVE', 'FPPOS', 'PROPOSID', 'OBSET_ID',

            # Data extension keywords
            'TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT'
        )

        for key in keys_that_should_be_there:
            assert key in test_datamodel.new_data

    def test_data_extension_data(self, test_datamodel):
        data_extension_keys = ('TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT')

        for key in data_extension_keys:
            assert isinstance(test_datamodel.new_data[key].values, np.ndarray)
