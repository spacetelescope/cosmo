import pytest
import os
import numpy as np

from astropy.io import fits
from shutil import copy

from cosmo.filesystem import (
    data_from_exposures,
    data_from_jitters,
    FileData,
    SPTData,
    ReferenceData,
    JitterFileData,
    find_files,
    get_exposure_data,
    get_jitter_data
)


@pytest.fixture(scope='class')
def file_data(data_dir):
    file = os.path.join(data_dir, 'ld1ce4dkq_lampflash.fits.gz')
    header_request = {0: ['ROOTNAME'], 1: ['EXPSTART']}
    table_request = {1: ['TIME', 'SHIFT_DISP', 'SHIFT_XDISP']}

    with fits.open(file) as f:
        test_data = FileData(f, header_request=header_request, table_request=table_request)

    return test_data


@pytest.fixture(scope='class')
def ref_data(data_dir):
    file = os.path.join(data_dir, 'ld1ce4dkq_lampflash.fits.gz')
    match_keys = ['OPT_ELEM', 'CENWAVE', 'FPOFFSET']
    header_request = {0: ['DETECTOR']}
    table_request = {1: ['SEGMENT', 'FP_PIXEL_SHIFT']}

    with fits.open(file) as f:
        ref_data = ReferenceData(
            f,
            'LAMPTAB',
            match_keys=match_keys,
            header_request=header_request,
            table_request=table_request
        )

    return ref_data


@pytest.fixture(scope='class')
def spt_data(data_dir):
    file = os.path.join(data_dir, 'ld3la1csq_rawacq.fits.gz')
    header_request = {0: ['INSTRUME']}

    return SPTData(file, header_request=header_request)


@pytest.fixture
def cosmo_layout_testdir(data_dir):
    # Set up the test cosmo-style directory
    cosmo_dir = os.path.join(data_dir, '11111')  # 11111 is a fake directory; matches the program directory pattern

    os.mkdir(cosmo_dir)
    copy(os.path.join(data_dir, 'lb4c10niq_lampflash.fits.gz'), cosmo_dir)

    yield

    os.remove(os.path.join(cosmo_dir, 'lb4c10niq_lampflash.fits.gz'))
    os.rmdir(cosmo_dir)


class TestFindFiles:

    def test_fails_for_bad_dir(self):
        assert not find_files('*', 'doesnotexist', subdir_pattern=None)

    def test_finds_files(self, data_dir):
        files = find_files('*lampflash*', data_dir=data_dir, subdir_pattern=None)

        assert len(files) == 11

    @pytest.mark.usefixtures("cosmo_layout_testdir")
    def test_finds_files_cosmo_layout(self, data_dir):
        files = find_files('*', data_dir=data_dir)

        assert len(files) == 1  # only one "program" directory with only one file in it


class TestFileData:

    def test_header_request(self, test_data):
        assert 'ROOTNAME' in test_data
        assert test_data['ROOTNAME'] == 'ld1ce4dkq'

    def test_table_request(self, test_data):
        assert 'TIME' in test_data and 'SHIFT_DISP' in test_data and 'SHIFT_XDISP' in test_data
        assert isinstance(test_data['TIME'],  np.ndarray)

    def test_header_request_fails_without_defaults(self, data_dir):
        file = os.path.join(data_dir, 'lb4c10niq_lampflash.fits.gz')
        header_request = {0: 'fake'}

        with pytest.raises(KeyError):
            with fits.open(file) as f:
                FileData(f, header_request=header_request)

    def test_header_request_defaults(self, data_dir):
        file = os.path.join(data_dir, 'lb4c10niq_lampflash.fits.gz')
        header_request = {0: 'fake'}
        header_defaults = {'fake': 'definitely fake'}

        with fits.open(file) as f:
            test_data = FileData(f, header_request=header_request, header_defaults=header_defaults)

        assert 'fake' in test_data
        assert test_data['fake'] == 'definitely fake'

    def test_bytestr_conversion(self, data_dir):
        file = os.path.join(data_dir, 'lb4c10niq_lampflash.fits.gz')
        header_request = {0: 'byte'}
        header_defaults = {'byte': np.array([b'here be bytes'])}

        with fits.open(file) as f:
            test_data = FileData(f, header_request=header_request, header_defaults=header_defaults)

        assert 'byte' in test_data
        assert test_data['byte'].dtype == np.unicode_

    def test_combine(self, test_data):
        to_combine = {'ROOTNAME': 'otherone'}

        combined = test_data.combine(to_combine, 'other')

        assert 'ROOTNAME' in combined and 'ROOTNAME_other' in combined
        assert combined['ROOTNAME'] == 'ld1ce4dkq' and combined['ROOTNAME_other'] == 'otherone'
        assert 'EXPSTART' in combined and 'TIME' in combined and 'SHIFT_DISP' in combined and 'SHIFT_XDISP' in combined


class TestReferenceData:

    def test_ref_data_match(self, ref_data):
        assert 'MATCH_COL' in ref_data and 'MATCH_VAL' in ref_data
        assert 'MATCH_VAL' == 'G230L'

    def test_header_request(self, ref_data):
        assert 'DETECTOR' in ref_data and ref_data['DETECTOR'] == 'NUV'

    def test_table_request(self, ref_data):
        assert 'SEGMENT' in ref_data and 'FP_PIXEL_SHIFT' in ref_data

        # With the match keys provided, three rows should be returned from the reference file.
        assert len(ref_data['SEGMENT']) == len(ref_data['FP_PIXEL_SHIFT']) == 3

    def test_bytestr_conversion(self, ref_data):
        assert ref_data['SEGMENT'].dtype == np.unicode_  # Default configuration converts the bytestr column to unicode


class TestSPTData:

    def test_header_request(self, spt_data):
        assert 'INSTRUME' in spt_data and spt_data['INSTRUME'] == 'COS'

    def test_filename_creation(self, data_dir):
        file = os.path.join(data_dir, 'ld3la1csq_rawacq.fits.gz')
        target = os.path.join(data_dir, 'ld3la1csq_spt.fits.gz')
        assert SPTData._create_spt_filename(file) == target

        file_without_gzip = os.path.join(data_dir, 'ld3la1csq_rawacq.fits')
        assert SPTData._create_spt_filename(file_without_gzip) == target

# TODO:
#  Add JitterFileData tests and test data.
#  Add get_exposure_data tests
#  Add get_jitter_data tests
#  Add data_from_exposures tests
#  Add data_from_jitters tests
