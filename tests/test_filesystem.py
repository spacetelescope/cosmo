import pytest
import os
import numpy as np

from astropy.io import fits

from cosmo.filesystem import get_file_data, FileData, FileDataFinder


BAD_INPUT = [
    # Different lengths in the data
    ('*', ['key1', 'key2'], [1], None, None, None, None),
    ('*', ['key1', 'key2'], [1, 1], ['key1', 'key2'], [1], None, None),
    ('*', ['key1', 'key2'], [1, 1], ['key1', 'key2'], [1, 1], [1], ['key1', 'key2']),

    # Input is missing corresponding extension argument with the keyword argument given
    ('*', ['key1', 'key2'], [1, 1], ['key1', 'key2'], None, None, None),
    ('*', ['key1', 'key2'], [1, 1], ['key1', 'key2'], [1, 1], None, ['key1', 'key2']),

    # Input is missing corresponding keyword argument with the extension argument given
    ('*', ['key1', 'key2'], [1, 1], None, [1, 1], None, None),
    ('*', ['key1', 'key2'], [1, 1], ['key1', 'key2'], [1, 1], [1, 1], None),
]


@pytest.fixture(params=BAD_INPUT)
def params(request):
    return request.param


class TestFileDataFinder:

    def test_fails_for_bad_input(self, params, data_dir):
        file_pattern, keywords, extensions, spt_keywords, spt_extensions, data_extensions, data_keys = params

        with pytest.raises(ValueError):
            FileDataFinder(
                data_dir,
                file_pattern,
                keywords,
                extensions,
                spt_keywords=spt_keywords,
                spt_extensions=spt_extensions,
                data_keywords=data_keys,
                data_extensions=data_extensions
            )

    def test_bad_input_dir_fails(self):
        with pytest.raises(OSError):
            FileDataFinder('doesnotexist', '*', ['key'], [0])

    def test_get_data_from_files(self, data_dir):
        test_finder = FileDataFinder(data_dir, '*lampflash*', ('ROOTNAME',), (0,), cosmo_layout=False)
        file_data = test_finder.get_data_from_files()

        assert None not in file_data
        assert len(file_data) == 11


@pytest.fixture
def testfile(data_dir):
    file = os.path.join(data_dir, 'lb4c10niq_lampflash.fits.gz')
    hdu = fits.open(file)
    testfile = FileData(
        file,
        hdu,
        ('ROOTNAME',),
        (0,),
        spt_keys=('LQTDFINI',),
        spt_exts=(1,),
        data_keys=('TIME',),
        data_exts=(1,)
    )

    yield testfile

    testfile.hdu.close()


class TestFileData:

    def test_spt_name(self, testfile, data_dir):
        assert testfile.spt_file is not None
        assert testfile.spt_file == os.path.join(data_dir, 'lb4c10niq_spt.fits.gz')

    def test_get_spt_header_data(self, testfile):
        testfile.get_spt_header_data()

        assert 'LQTDFINI' in testfile.data.keys()
        assert testfile.data['LQTDFINI'] == 'TDF Up'

    def test_get_header_data(self, testfile):
        testfile.get_header_data()

        assert 'ROOTNAME' in testfile.data.keys()
        assert testfile.data['ROOTNAME'] == 'lb4c10niq'

    def test_get_table_data(self, testfile):
        testfile.get_table_data()

        assert 'TIME' in testfile.data.keys()
        assert isinstance(testfile.data['TIME'],  np.ndarray)


@pytest.fixture
def delayed_get_data(data_dir):
    file = os.path.join(data_dir, 'lb4c10niq_lampflash.fits.gz')

    delayed_get_data = get_file_data(
        file,
        ('ROOTNAME',),
        (0,),
        spt_keys=('LQTDFINI',),
        spt_exts=(1,),
        data_keys=('TIME',),
        data_exts=(1,)
    )

    return delayed_get_data


class TestGetFileData:

    def test_compute_result(self, delayed_get_data):
        result = delayed_get_data.compute(scheduler='multiprocessing')

        assert isinstance(result, dict)
        assert 'ROOTNAME' in result and 'LQTDFINI' in result and 'TIME' in result
        assert result['ROOTNAME'] == 'lb4c10niq'
        assert result['LQTDFINI'] == 'TDF Up'
        assert isinstance(result['TIME'], np.ndarray)
