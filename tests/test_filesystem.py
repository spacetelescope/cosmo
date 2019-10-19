import pytest
import os
import numpy as np

from shutil import copy

from cosmo.filesystem import get_file_data, FileData, find_files


BAD_INPUT = [
    # Different lengths in the data
    (['key1', 'key2'], [1], None, None, None, None, None),
    (['key1', 'key2'], [1, 1], ['key1', 'key2'], [1], None, None, None),
    (['key1', 'key2'], [1, 1], ['key1', 'key2'], [1, 1], [1], ['key1', 'key2'], None),

    # Input is missing corresponding extension argument with the keyword argument given
    (['key1', 'key2'], [1, 1], ['key1', 'key2'], None, None, None, None),
    (['key1', 'key2'], [1, 1], ['key1', 'key2'], [1, 1], None, ['key1', 'key2'], None),

    # Input is missing corresponding keyword argument with the extension argument given
    (['key1', 'key2'], [1, 1], None, [1, 1], None, None, None),
    (['key1', 'key2'], [1, 1], ['key1', 'key2'], [1, 1], [1, 1], None, None),

    # Input has bad reference_request dictionary
    (
        ['key1', 'key2'],
        [1, 1],
        ['key1', 'key2'],
        [1, 1],
        [1, 1],
        ['key1', 'key2'],
        {'reference': 'test', 'match': ['test']}
    ),
    (
        ['key1', 'key2'],
        [1, 1],
        ['key1', 'key2'],
        [1, 1],
        [1, 1],
        ['key1', 'key2'],
        {'match': ['test'], 'columns': ['test']}
    ),
    (
        ['key1', 'key2'],
        [1, 1],
        ['key1', 'key2'],
        [1, 1],
        [1, 1],
        ['key1', 'key2'],
        {'reference': 'test', 'columns': ['test']}
    ),
    (
        ['key1', 'key2'],
        [1, 1],
        ['key1', 'key2'],
        [1, 1],
        [1, 1],
        ['key1', 'key2'],
        {'reference': 'test', 'match': ['test'], 'columns': 'test'}
    ),
    (
        ['key1', 'key2'],
        [1, 1],
        ['key1', 'key2'],
        [1, 1],
        [1, 1],
        ['key1', 'key2'],
        {'reference': 'test', 'match': 'test', 'columns': ['test']}
    ),
]


@pytest.fixture(params=BAD_INPUT)
def params(request):
    return request.param


@pytest.fixture
def testfiledata(data_dir):
    file = os.path.join(data_dir, 'lb4c10niq_lampflash.fits.gz')
    testfile = FileData(
        file,
        ('ROOTNAME',),
        (0,),
        spt_keywords=('LQTDFINI',),
        spt_extensions=(1,),
        data_keywords=('TIME', 'SEGMENT'),
        data_extensions=(1, 1),
        reference_request={
            'LAMPTAB': {
                'match': ['OPT_ELEM', 'CENWAVE', 'FPOFFSET'],
                'columns': ['SEGMENT', 'FP_PIXEL_SHIFT']
            }
        }
    )

    return testfile


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
        with pytest.raises(OSError):
            find_files('*', 'doesnotexist', cosmo_layout=False)

    def test_finds_files(self, data_dir):
        files = find_files('*lampflash*', data_dir=data_dir, cosmo_layout=False)

        assert len(files) == 11

    @pytest.mark.usefixtures("cosmo_layout_testdir")
    def test_finds_files_cosmo_layout(self, data_dir):
        files = find_files('*', data_dir=data_dir, cosmo_layout=True)

        assert len(files) == 1  # only one "program" directory with only one file in it


class TestFileData:

    def test_spt_name(self, testfiledata, data_dir):
        assert (
                testfiledata._create_spt_filename(testfiledata['FILENAME'], 'spt.fits.gz') ==
                os.path.join(data_dir, 'lb4c10niq_spt.fits.gz')
        )

    def test_missing_spt_returns_none(self, data_dir):
        file = os.path.join(data_dir, 'lbhx26fmq_lampflash.fits.gz')  # This file doesn't have a matching spt file

        assert FileData._create_spt_filename(file, 'spt.fits.gz') is None

    def test_get_spt_header_data(self, testfiledata):
        assert 'LQTDFINI' in testfiledata
        assert testfiledata['LQTDFINI'] == 'TDF Up'

    def test_get_header_data(self, testfiledata):
        assert 'ROOTNAME' in testfiledata
        assert testfiledata['ROOTNAME'] == 'lb4c10niq'

    def test_get_table_data(self, testfiledata):
        assert 'TIME' in testfiledata and 'SEGMENT' in testfiledata
        assert isinstance(testfiledata['TIME'],  np.ndarray)

    def test_get_reference_data(self, testfiledata):
        assert 'SEGMENT' in testfiledata and 'SEGMENT_ref' in testfiledata
        assert 'FP_PIXEL_SHIFT' in testfiledata

    def test_get_reference_data_fails_match(self, data_dir):
        file = os.path.join(data_dir, 'lb4c10niq_lampflash.fits.gz')

        with pytest.raises(ValueError):
            FileData(
                file,
                ('ROOTNAME',),
                (0,),
                spt_keywords=('LQTDFINI',),
                spt_extensions=(1,),
                data_keywords=('TIME', 'SEGMENT'),
                data_extensions=(1, 1),
                reference_request={
                    'LAMPTAB': {
                        'match': ['SEGMENT'],  # SEGMENT is N/A for the test file; lamptab doesn't have a N/A entry
                        'columns': ['SEGMENT', 'FP_PIXEL_SHIFT']
                    }
                }
            )

    def test_get_multiple_references(self, data_dir):
        file = os.path.join(data_dir, 'lb4c10niq_lampflash.fits.gz')

        FileData(
            file,
            ('ROOTNAME',),
            (0,),
            spt_keywords=('LQTDFINI',),
            spt_extensions=(1,),
            data_keywords=('TIME', 'SEGMENT'),
            data_extensions=(1, 1),
            reference_request={
                'LAMPTAB': {
                    'match': ['OPT_ELEM', 'CENWAVE', 'FPOFFSET'],
                    'columns': ['SEGMENT', 'FP_PIXEL_SHIFT']
                },
                'XTRACTAB': {
                    'match': ['OPT_ELEM', 'CENWAVE', 'APERTURE'],
                    'columns': ['SLOPE', 'HEIGHT', 'SEGMENT']
                }
            }
        )

    def test_fails_with_bad_input(self, data_dir, params):
        file = os.path.join(data_dir, 'lb4c10niq_lampflash.fits.gz')
        header_keys, header_exts, spt_keys, spt_exts, data_exts, data_keys, reference_request = params

        # noinspection PyTypeChecker
        with pytest.raises((ValueError, TypeError)):
            FileData(
                file,
                header_keywords=header_keys,
                header_extensions=header_exts,
                spt_keywords=spt_keys,
                spt_extensions=spt_exts,
                data_keywords=data_keys,
                data_extensions=data_exts,
                reference_request=reference_request
            )


class TestGetFileData:

    def test_compute_result(self, data_dir):
        files = [os.path.join(data_dir, 'lb4c10niq_lampflash.fits.gz')]

        result = get_file_data(
            files,
            ('ROOTNAME',),
            (0,),
            spt_keywords=('LQTDFINI',),
            spt_extensions=(1,),
            data_keywords=('TIME',),
            data_extensions=(1,)
        )

        assert isinstance(result, list) and len(result) == 1
        assert 'ROOTNAME' in result[0] and 'LQTDFINI' in result[0] and 'TIME' in result[0]
        assert result[0]['ROOTNAME'] == 'lb4c10niq'
        assert result[0]['LQTDFINI'] == 'TDF Up'
        assert isinstance(result[0]['TIME'], np.ndarray)
