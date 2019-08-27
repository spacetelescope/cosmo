import pandas as pd
import numpy as np
import pytest

from cosmo.monitors.data_models import AcqDataModel, OSMDataModel
from cosmo.sms import SMSFinder

# TODO: Write tests for data model ingest()


@pytest.fixture
def make_datamodel(data_dir):
    def _make_datamodel(model):
        if model == OSMDataModel:
            # OSM Drift data model requires that an SMS database exist
            test_finder = SMSFinder(data_dir)
            test_finder.ingest_files()

        model.files_source = data_dir
        model.cosmo_layout = False
        test_model = model()

        return test_model

    return _make_datamodel


class TestOSMDataModel:

    @pytest.fixture(autouse=True)
    def osmmodel(self, request, make_datamodel):
        osmmodel = make_datamodel(OSMDataModel)

        request.cls.osmmodel = osmmodel  # Add the data model to the test class

    def test_data_collection(self):
        assert isinstance(self.osmmodel.new_data, pd.DataFrame)
        assert len(self.osmmodel.new_data) == 11  # There are 11 test data sets

    def test_content_collected(self):
        keys_that_should_be_there = (
            # Header keywords
            'ROOTNAME', 'EXPSTART', 'DETECTOR', 'LIFE_ADJ', 'OPT_ELEM', 'CENWAVE', 'FPPOS', 'PROPOSID', 'OBSET_ID',

            # Data extension keywords
            'TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT',

            # Data from the SMS files
            'TSINCEOSM1', 'TSINCEOSM2'
        )

        for key in keys_that_should_be_there:
            assert key in self.osmmodel.new_data

        # Check that entries that have no data have been removed
        assert not self.osmmodel.new_data.apply(lambda x: not bool(len(x.SHIFT_DISP)), axis=1).all()

    def test_data_extension_data(self):
        data_extension_keys = ('TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT')

        for key in data_extension_keys:
            assert isinstance(self.osmmodel.new_data[key].values, np.ndarray)


class TestAcqDataModel:

    @pytest.fixture(autouse=True)
    def acqmodel(self, request, make_datamodel):
        acqmodel = make_datamodel(AcqDataModel)

        request.cls.acqmodel = acqmodel

    def test_data_collection(self):
        keys_that_should_be_there = (
            # Header keywords
            'ACQSLEWX', 'ACQSLEWY', 'EXPSTART', 'ROOTNAME', 'PROPOSID', 'OBSTYPE', 'NEVENTS', 'SHUTTER', 'LAMPEVNT',
            'ACQSTAT', 'EXTENDED', 'LINENUM', 'APERTURE', 'OPT_ELEM', 'CENWAVE', 'DETECTOR', 'LIFE_ADJ',

            # SPT header keywords
            'DGESTAR'
        )

        for key in keys_that_should_be_there:
            assert key in self.acqmodel.new_data

        # Check that the FGS column was created correctly
        assert 'FGS' in self.acqmodel.new_data
        assert sorted(self.acqmodel.new_data.FGS.unique()) == ['F1', 'F2', 'F3']
