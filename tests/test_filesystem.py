import pytest
import os
import numpy as np

from astropy.io import fits

from cosmo.filesystem import get_file_data, FileData, FileDataFinder

TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/')
TEST_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cosmoconfig_test.yaml')

# Check to make sure that the test config file is being used. If not, don't run the tests
if os.path.abspath(os.environ['COSMO_CONFIG']) != TEST_CONFIG:
    raise TypeError('Tests should only be executed with the testing configuration file')

ARGS = (
    'source_dr',
    'file_pattern',
    'keywords',
    'extensions',
    'spt_keywords',
    'spt_extensions',
    'data_extensions',
    'data_keys'
)

DIFFERENT_LENGTHS = (
    ARGS,
    [
        (TEST_DATA, '*', ['key1', 'key2'], [1], None, None, None, None),
        (TEST_DATA, '*', ['key1', 'key2'], [1, 1], ['key1', 'key2'], [1], None, None),
        (TEST_DATA, '*', ['key1', 'key2'], [1, 1], ['key1', 'key2'], [1, 1], [1], ['key1', 'key2'])
    ]
)

MISSING_EXTS = (
    ARGS,
    [
        (TEST_DATA, '*', ['key1', 'key2'], [1, 1], ['key1', 'key2'], None, None, None),
        (TEST_DATA, '*', ['key1', 'key2'], [1, 1], ['key1', 'key2'], [1, 1], None, ['key1', 'key2'])
    ]
)

MISSING_KEYS = (
    ARGS,
    [
        (TEST_DATA, '*', ['key1', 'key2'], [1, 1], None, [1, 1], None, None),
        (TEST_DATA, '*', ['key1', 'key2'], [1, 1], ['key1', 'key2'], [1, 1], [1, 1], None)
    ]
)


class TestFileDataFinder:

    @pytest.mark.parametrize(*DIFFERENT_LENGTHS)
    def test_fails_for_different_legnths(self, source_dr, file_pattern, keywords, extensions, spt_keywords,
                                         spt_extensions, data_extensions, data_keys):
        with pytest.raises(ValueError):
            FileDataFinder(
                source_dr,
                file_pattern,
                keywords,
                extensions,
                spt_keywords=spt_keywords,
                spt_extensions=spt_extensions,
                data_keywords=data_keys,
                data_extensions=data_extensions
            )

    @pytest.mark.parametrize(*MISSING_EXTS)
    def test_fails_for_missing_extensions(self, source_dr, file_pattern, keywords, extensions, spt_keywords,
                                          spt_extensions, data_extensions, data_keys):

        with pytest.raises(TypeError):
            FileDataFinder(
                source_dr,
                file_pattern,
                keywords,
                extensions,
                spt_keywords=spt_keywords,
                spt_extensions=spt_extensions,
                data_keywords=data_keys,
                data_extensions=data_extensions
            )

    @pytest.mark.parametrize(*MISSING_KEYS)
    def test_fails_for_missing_keywords(self, source_dr, file_pattern, keywords, extensions, spt_keywords,
                                        spt_extensions, data_extensions, data_keys):
        with pytest.raises(TypeError):
            FileDataFinder(
                source_dr,
                file_pattern,
                keywords,
                extensions,
                spt_keywords=spt_keywords,
                spt_extensions=spt_extensions,
                data_keywords=data_keys,
                data_extensions=data_extensions
            )

    def test_get_data_from_files(self):
        test_finder = FileDataFinder(TEST_DATA, '*rawtag*', ('ROOTNAME',), (0,), cosmo_layout=False)
        file_data = test_finder.get_data_from_files()

        assert None not in file_data
        assert len(file_data) == 4


class TestFileData:

    @classmethod
    def setup_class(cls):
        file = os.path.join(TEST_DATA, 'ldfd01vpq_rawtag_a.fits.gz')
        hdu = fits.open(file)
        cls.file_data = FileData(
            file,
            hdu,
            ('ROOTNAME',),
            (0,),
            spt_keys=('LQTDFINI',),
            spt_exts=(1,),
            data_keys=('RAWX',),
            data_exts=(1,)
        )

    # noinspection PyUnresolvedReferences
    @classmethod
    def teardown_class(cls):
        cls.file_data.hdu.close()

    def test_spt_name(self):
        assert self.file_data.spt_file is not None
        assert self.file_data.spt_file == os.path.join(TEST_DATA, 'ldfd01vpq_spt.fits.gz')

    def test_get_spt_header_data(self):
        self.file_data.get_spt_header_data()

        assert 'LQTDFINI' in self.file_data.data.keys()
        assert self.file_data.data['LQTDFINI'] == 'TDF Up'

    def test_get_header_data(self):
        self.file_data.get_header_data()

        assert 'ROOTNAME' in self.file_data.data.keys()
        assert self.file_data.data['ROOTNAME'] == 'ldfd01vpq'

    def test_get_table_data(self):
        self.file_data.get_table_data()

        assert 'RAWX' in self.file_data.data.keys()
        assert isinstance(self.file_data.data['RAWX'],  np.ndarray)


class TestGetFileData:

    @classmethod
    def setup_class(cls):
        cls.file = os.path.join(TEST_DATA, 'ldfd01vpq_rawtag_a.fits.gz')

        cls.delayed_result = get_file_data(
            cls.file,
            ('ROOTNAME',),
            (0,),
            spt_keys=('LQTDFINI',),
            spt_exts=(1,),
            data_keys=('RAWX',),
            data_exts=(1,)
        )

    def test_files_are_closed(self):
        # if the file is not closed, this will produce an error. However, different processes might have the file
        # open and this wouldn't catch that
        f = open(self.file)
        f.close()

        assert f.closed

    def test_compute_result(self):
        result = self.delayed_result.compute(scheduler='multiprocessing')

        assert isinstance(result, dict)
        assert 'ROOTNAME' in result and 'LQTDFINI' in result and 'RAWX' in result
        assert result['ROOTNAME'] == 'ldfd01vpq'
        assert result['LQTDFINI'] == 'TDF Up'
        assert isinstance(result['RAWX'], np.ndarray)


# class TestFindFiles:
#
#     def test
