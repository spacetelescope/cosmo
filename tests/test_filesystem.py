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


@pytest.fixture(params=['ldngz2szj_jit.fits.gz', 'ldxe02010_jit.fits.gz'], scope='class')
def jitter_file(data_dir, request):
    file = os.path.join(data_dir, request.param)
    primary_header_keys = ['PROPOSID', 'CONFIG']
    ext_header_keys = ['EXPNAME']
    table_keys = ['SI_V2_AVG']

    return JitterFileData(
        file,
        primary_header_keys=primary_header_keys,
        ext_header_keys=ext_header_keys,
        table_keys=table_keys
    )


@pytest.fixture(scope='class')
def exposure_data(data_dir):
    file = os.path.join(data_dir, 'lb4c10niq_lampflash.fits.gz')
    header_request = {0: ['ROOTNAME'], 1: ['EXPSTART']}
    table_request = {1: ['TIME', 'SHIFT_DISP']}
    spt_header_request = {1: ['LQTDFINI']}
    reference_request = {
        'LAMPTAB': {
            'match_keys': ['OPT_ELEM', 'CENWAVE', 'FPOFFSET'],
            'table_request': {1: ['FP_PIXEL_SHIFT', 'SEGMENT']}
        },
        'WCPTAB': {
            'match_keys': ['OPT_ELEM'],
            'table_request': {1: ['XC_RANGE', 'SEARCH_OFFSET']}
        }
    }

    return get_exposure_data(
        file,
        header_request=header_request,
        table_request=table_request,
        spt_header_request=spt_header_request,
        reference_request=reference_request
    )


@pytest.fixture(scope='class')
def jitter_data(data_dir):
    file = os.path.join(data_dir, 'ldxe02010_jit.fits.gz')
    primary_header_keys = ['PROPOSID']
    ext_header_keys = ['EXPNAME']
    table_keys = ['SI_V2_AVG']
    reduce_to_stats = {'SI_V2_AVG': ('mean', 'std', 'max')}

    return get_jitter_data(
        file,
        primary_header_keys=primary_header_keys,
        ext_header_keys=ext_header_keys,
        table_keys=table_keys,
        get_expstart=False,
        reduce_to_stats=reduce_to_stats
    )


@pytest.fixture(scope='class')
def multi_exposure_data(data_dir):
    files = find_files('*rawacq*', data_dir=data_dir, subdir_pattern=None)
    header_request = {0: ['ROOTNAME']}

    return data_from_exposures(files, header_request=header_request)


@pytest.fixture(scope='class')
def multi_jitter_data(data_dir):
    files = find_files('*jit*', data_dir=data_dir, subdir_pattern=None)
    primary_header_keys = ['PROPOSID']
    ext_header_keys = ['EXPNAME']

    return data_from_jitters(
        files,
        primary_header_keys=primary_header_keys,
        ext_header_keys=ext_header_keys,
        get_expstart=False
    )


class TestFindFiles:

    def test_fails_for_bad_dir(self):
        assert not find_files('*', 'doesnotexist', subdir_pattern=None)

    def test_finds_files(self, data_dir):
        files = find_files('*lampflash*', data_dir=data_dir, subdir_pattern=None)

        assert len(files) == 11

    @pytest.mark.usefixtures("cosmo_layout_testdir")
    def test_finds_files_cosmo_layout(self, data_dir):
        files = find_files('*', data_dir=data_dir)

        assert len(files) == 51  # only one "program" directory with only one file in it


class TestFileData:

    def test_header_request(self, file_data):
        assert 'ROOTNAME' in file_data
        assert file_data['ROOTNAME'] == 'ld1ce4dkq'

    def test_table_request(self, file_data):
        assert 'TIME' in file_data and 'SHIFT_DISP' in file_data and 'SHIFT_XDISP' in file_data
        assert isinstance(file_data['TIME'], np.ndarray)

    def test_header_request_fails_without_defaults(self, data_dir):
        file = os.path.join(data_dir, 'lb4c10niq_lampflash.fits.gz')
        header_request = {0: ['fake']}

        with pytest.raises(KeyError):
            with fits.open(file) as f:
                FileData(f, header_request=header_request)

    def test_header_request_defaults(self, data_dir):
        file = os.path.join(data_dir, 'lb4c10niq_lampflash.fits.gz')
        header_request = {0: ['fake']}
        header_defaults = {'fake': 'definitely fake'}

        with fits.open(file) as f:
            test_data = FileData(f, header_request=header_request, header_defaults=header_defaults)

        assert 'fake' in test_data
        assert test_data['fake'] == 'definitely fake'

    def test_bytestr_conversion(self, data_dir):
        file = os.path.join(data_dir, 'lb4c10niq_lampflash.fits.gz')
        header_request = {0: ['byte']}
        header_defaults = {'byte': np.array([b'here be bytes'])}

        with fits.open(file) as f:
            test_data = FileData(f, header_request=header_request, header_defaults=header_defaults)

        assert 'byte' in test_data
        assert test_data['byte'].dtype == np.dtype('<U13')

    def test_combine(self, file_data):
        to_combine = {'ROOTNAME': 'otherone'}
        file_data.combine(to_combine, 'other')

        assert 'ROOTNAME' in file_data and 'other_ROOTNAME' in file_data
        assert file_data['ROOTNAME'] == 'ld1ce4dkq' and file_data['other_ROOTNAME'] == 'otherone'
        assert (
            'EXPSTART' in file_data and 'TIME' in file_data and 'SHIFT_DISP' in file_data and 'SHIFT_XDISP' in file_data
        )


class TestReferenceData:

    def test_ref_data_match(self, ref_data):
        assert ref_data.match_values == {'OPT_ELEM': 'G130M', 'CENWAVE': 1291, 'FPOFFSET': -1}

    def test_header_request(self, ref_data):
        assert 'DETECTOR' in ref_data and ref_data['DETECTOR'] == 'FUV'

    def test_table_request(self, ref_data):
        assert 'SEGMENT' in ref_data and 'FP_PIXEL_SHIFT' in ref_data

        # With the match keys provided, three rows should be returned from the reference file.
        assert len(ref_data['SEGMENT']) == len(ref_data['FP_PIXEL_SHIFT']) == 2

    def test_bytestr_conversion(self, ref_data):
        assert ref_data['SEGMENT'].dtype == np.dtype('<U4')  # Default converts the bytestr column to unicode


class TestSPTData:

    def test_header_request(self, spt_data):
        assert 'INSTRUME' in spt_data and spt_data['INSTRUME'] == 'COS'

    def test_filename_creation(self, data_dir):
        file = os.path.join(data_dir, 'ld3la1csq_rawacq.fits.gz')
        target = os.path.join(data_dir, 'ld3la1csq_spt.fits.gz')
        assert SPTData._create_spt_filename(file) == target

        file_without_gzip = os.path.join(data_dir, 'ld3la1csq_rawacq.fits')
        assert SPTData._create_spt_filename(file_without_gzip) == target


class TestJitterFileData:

    def test_header_request(self, jitter_file):
        for data in jitter_file:
            if len(jitter_file) == 1:
                assert 'EXPSTART' in data and data['EXPSTART'] == 58486.19196402
                assert 'EXPTYPE' in data and data['EXPTYPE'] == 'ACQ/PEAKXD'

            if len(jitter_file) > 1:
                assert data['EXPSTART'] == 0 and data['EXPTYPE'] == 'N/A'  # associated raw files are not in the dataset

            assert 'PROPOSID' in data and 'CONFIG' in data and 'EXPNAME' in data

    def test_extension_data(self, jitter_file):
        if len(jitter_file) > 1:
            assert len(jitter_file) == 4  # the jitter association file includes 4 extensions apart from the primary

    def test_table_request(self, jitter_file):
        for data in jitter_file:
            assert 'SI_V2_AVG' in data
            assert all(data['SI_V2_AVG'] < 1e30)  # Make sure placeholder data is properly removed

    def test_reduce_to_stat(self, jitter_file):
        reduce = {'SI_V2_AVG': ('mean', 'std', 'max')}

        jitter_file.reduce_to_stat(reduce)

        for data in jitter_file:
            assert 'SI_V2_AVG' not in data
            assert 'SI_V2_AVG_mean' in data and 'SI_V2_AVG_std' in data and 'SI_V2_AVG_max' in data


class TestGetExposureData:

    def test_recovered_length(self, exposure_data):
        assert len(exposure_data) == 10  # All request keys/columns

    def test_header_requests(self, exposure_data):
        assert 'ROOTNAME' in exposure_data and exposure_data['ROOTNAME'] == 'lb4c10niq'
        assert 'EXPSTART' in exposure_data and exposure_data['EXPSTART'] == 55202.48302439
        assert 'LQTDFINI' in exposure_data and exposure_data['LQTDFINI'] == 'TDF Up'

    def test_table_requests(self, exposure_data):
        assert (
                'TIME' in exposure_data and
                np.allclose(  # Using isclose due to dtype discrepancies
                    exposure_data['TIME'],
                    [4.32000017, 4.32000017, 4.32000017, 2404.35205078, 2404.35205078, 2404.35205078]
                )
        )

        assert (
                'SHIFT_DISP' in exposure_data and
                np.allclose(
                    exposure_data['SHIFT_DISP'],
                    [-23.67234, -23.992914, -24.230333, -23.85408, -23.90261, -24.0943]
                )
        )

        assert 'SEGMENT' in exposure_data and len(exposure_data['SEGMENT']) == 3  # NUV
        assert 'FP_PIXEL_SHIFT' in exposure_data and len(exposure_data['FP_PIXEL_SHIFT']) == 3
        assert 'XC_RANGE' in exposure_data
        assert 'SEARCH_OFFSET' in exposure_data


class TestDataFromExposures:

    def test_length(self, multi_exposure_data):
        assert len(multi_exposure_data) == 9

    def test_data_collection(self, multi_exposure_data):
        test_data = sorted([filedata['ROOTNAME'] for filedata in multi_exposure_data])
        actual = sorted(
            [
                'ld3la1csq',
                'ldngz2szq',
                'ldi404zsq',
                'ldi405e3q',
                'lb4l02m1q',
                'lbhx1epiq',
                'ldng01chq',
                'lb4l02m2q',
                'ldng05huq'
            ]
        )

        assert test_data == actual


class TestGetJitterData:

    def test_length(self, jitter_data):
        assert len(jitter_data) == 4

    def test_header_data(self, jitter_data):
        for data in jitter_data:
            assert 'PROPOSID' in data and 'EXPNAME' in data

    def test_table_data(self, jitter_data):
        for data in jitter_data:
            assert 'SI_V2_AVG_mean' in data and 'SI_V2_AVG_std' in data and 'SI_V2_AVG_max' in data
            assert 'SI_V2_AVG' not in data


class TestDataFromJitters:

    def test_length(self, multi_jitter_data):
        assert len(multi_jitter_data) == 6

    def test_data(self, multi_jitter_data):
        for data in multi_jitter_data:
            assert 'PROPOSID' in data and 'EXPNAME' in data
