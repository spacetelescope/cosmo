import pandas as pd
import numpy as np
import pytest

from cosmo.monitors.osm_data_models import OSMShiftDataModel, OSMDriftDataModel
from cosmo.monitors.acq_data_models import AcqPeakdModel, AcqImageModel, AcqPeakxdModel
from cosmo.sms import SMSFinder


@pytest.fixture
def make_datamodel(data_dir):
    def _make_datamodel(model):
        if model == OSMDriftDataModel:
            # OSM Drift data model requires that an SMS database exist
            test_finder = SMSFinder(data_dir)
            test_finder.ingest_files()

        model.files_source = data_dir
        model.cosmo_layout = False
        test_model = model()

        return test_model

    return _make_datamodel


@pytest.fixture(params=[AcqPeakxdModel, AcqPeakdModel, AcqImageModel])
def acqmodel(request, make_datamodel):
    return make_datamodel(request.param)


def test_fgs_column(acqmodel):
    assert 'FGS' in acqmodel.new_data
    assert sorted(acqmodel.new_data.FGS.unique()) == ['F1', 'F2', 'F3']


class TestOSMShiftDataModel:

    @pytest.fixture(autouse=True)
    def osmshiftmodel(self, request, make_datamodel):
        osmshiftmodel = make_datamodel(OSMShiftDataModel)

        request.cls.osmshiftmodel = osmshiftmodel  # Add the data model to the test class

    def test_data_collection(self):
        assert isinstance(self.osmshiftmodel.new_data, pd.DataFrame)
        assert len(self.osmshiftmodel.new_data) == 11  # There are 11 test data sets

    def test_content_collected(self):
        keys_that_should_be_there = (
            # Header keywords
            'ROOTNAME', 'EXPSTART', 'DETECTOR', 'LIFE_ADJ', 'OPT_ELEM', 'CENWAVE', 'FPPOS', 'PROPOSID', 'OBSET_ID',

            # Data extension keywords
            'TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT'
        )

        for key in keys_that_should_be_there:
            assert key in self.osmshiftmodel.new_data

    def test_data_extension_data(self):
        data_extension_keys = ('TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT')

        for key in data_extension_keys:
            assert isinstance(self.osmshiftmodel.new_data[key].values, np.ndarray)


class TestOSMDriftDataModel:

    @pytest.fixture(autouse=True)
    def osmdriftmodel(self, request, make_datamodel):
        osmdriftmodel = make_datamodel(OSMDriftDataModel)

        request.cls.osmdriftmodel = osmdriftmodel

    def test_data_collention(self):
        assert isinstance(self.osmdriftmodel.new_data, pd.DataFrame)
        assert len(self.osmdriftmodel.new_data) == 11

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
            assert key in self.osmdriftmodel.new_data

        # Check that entries that have no data have been removed
        assert not self.osmdriftmodel.new_data.apply(lambda x: not bool(len(x.SHIFT_DISP)), axis=1).all()


class TestAcqImageModels:

    @pytest.fixture(autouse=True)
    def acqimagemodel(self, request, make_datamodel):
        acqimagemodel = make_datamodel(AcqImageModel)

        request.cls.acqimagemodel = acqimagemodel

    def test_data_collection(self):
        keys_that_should_be_there = (
            # Header keywords
            'ACQSLEWX', 'ACQSLEWY', 'EXPSTART', 'ROOTNAME', 'PROPOSID', 'OBSTYPE', 'NEVENTS', 'SHUTTER', 'LAMPEVNT',
            'ACQSTAT', 'EXTENDED', 'LINENUM', 'APERTURE', 'OPT_ELEM',

            # SPT header keywords
            'DGESTAR',

            # Generated keys
            'V2SLEW', 'V3SLEW'
        )

        for key in keys_that_should_be_there:
            assert key in self.acqimagemodel.new_data


class TestAcqPeakdModel:

    @pytest.fixture(autouse=True)
    def acqpeakdmodel(self, request, make_datamodel):
        acqpeakdmodel = make_datamodel(AcqPeakdModel)

        request.cls.acqpeakdmodel = acqpeakdmodel

    def test_data_collection(self):
        keys_that_should_be_there = (
            # Header keywords
            'ACQSLEWX', 'EXPSTART', 'LIFE_ADJ', 'ROOTNAME', 'PROPOSID', 'OPT_ELEM', 'CENWAVE', 'DETECTOR',

            # SPT keywords
            'DGESTAR'
        )

        for key in keys_that_should_be_there:
            assert key in self.acqpeakdmodel.new_data


class TestAcqPeakxdModel:

    @pytest.fixture(autouse=True)
    def acqpeakxdmodel(self, request, make_datamodel):
        acqpeakxdmodel = make_datamodel(AcqPeakxdModel)

        request.cls.acqpeakxdmodel = acqpeakxdmodel

    def test_data_collection(self):
        keys_that_should_be_there = (
            # Header keywords
            'ACQSLEWY', 'EXPSTART', 'LIFE_ADJ', 'ROOTNAME', 'PROPOSID', 'OPT_ELEM', 'CENWAVE', 'DETECTOR',

            # SPT keywords
            'DGESTAR'
        )

        for key in keys_that_should_be_there:
            assert key in self.acqpeakxdmodel.new_data
