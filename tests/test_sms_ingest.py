import pytest
import os

from cosmo.sms import SMSFinder, SMSFile, SMSFileStats, SMSTable, ingest_sms_data, DB

TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/')
TEST_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test.db')
TEST_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cosmoconfig_test.yaml')

# Check to make sure that the test config file is being used. If not, don't run the tests
if os.environ['COSMO_CONFIG'] != TEST_CONFIG:
    raise TypeError('Tests should only be executed with the testing configuration file')

SMSFinder_BAD_PATHS = (
    ('source',),
    [
        (os.path.dirname(os.path.abspath(__file__)),),  # directory exists, but no files should be found
        ('/this/is/not/a/directory',)  # directory doesn't exist
    ]
)

# Need to test SMSFile on 3 different sms files: old-style (pre 2015 or something), new-style, and alternate suffix
SMSFile_GOOD_DATA = (
    ('file',),
    [
        (os.path.join(TEST_DATA, test_file),) for test_file in ['111078a6.txt', '180147b1.txt', '180148a5.l-exp']
    ]
)

class TestIngestSmsData:

    @classmethod
    def teardown_class(cls):
        DB.drop_tables([SMSFileStats, SMSTable])

    def test_cold_start(self):
        ingest_sms_data(TEST_DATA, cold_start=True)


class TestSMSFile:

    @classmethod
    def teardown_class(cls):
        DB.drop_tables([SMSFileStats, SMSTable])

    @pytest.mark.parametrize(*SMSFile_GOOD_DATA)
    def test_data_ingest(self, file):
        SMSFile(file)

    def test_ingest_fail(self):
        bad_file = os.path.join(TEST_DATA, 'bad_111078a6.txt')

        with pytest.raises(ValueError):
            SMSFile(bad_file)

    @pytest.mark.parametrize(*SMSFile_GOOD_DATA)
    def test_datatypes(self, file):
        correct_dtypes = {
            'FILENAME': object,
            'ROOTNAME': object,
            'PROPOSID': int,
            'DETECTOR': object,
            'OPMODE': object,
            'EXPTIME': float,
            'EXPSTART': object,
            'FUVHVSTATE': object,
            'APERTURE': object,
            'OSM1POS': object,
            'OSM2POS': object,
            'CENWAVE': int,
            'FPPOS': int,
            'TSINCEOSM1': float,
            'TSINCEOSM2': float
        }

        sms = SMSFile(file)
        dtypes = sms.data.dtypes

        for key, value in dtypes.iteritems():
            assert value == correct_dtypes[key]

    @pytest.mark.parametrize(*SMSFile_GOOD_DATA)
    def test_database_ingest(self, file):
        test_sms = SMSFile(file)
        test_sms.insert_to_db()


class TestSMSFinder:
    """Tests for SMSFinder"""

    @classmethod
    def setup_class(cls):  # TODO: Refactor to use fixtures instead of setup/teardown class methods
        """Set up the tests with a 'test' instance of the SMSFinder."""
        ingest_sms_data(TEST_DATA, cold_start=True)  # ingest the files
        cls.test_finder = SMSFinder(TEST_DATA)  # Create a finder instance

    @classmethod
    def teardown_class(cls):
        DB.drop_tables([SMSFileStats, SMSTable])

    def test_found(self):
        """Test that sms files are found correctly."""
        assert len(self.test_finder.all_sms) == 3

    def test_new_sms(self):
        """Test that the sms files are correctly determined as new."""
        # test files were all moved on the same day, and the "test" day and the last_ingest date are the same,
        # so they're all old
        assert self.test_finder.new_sms is None

    def test_old_sms(self):
        """Test that sms files are corrctly determined as old."""
        # test files were all moved on the same day as the "test" day and the last_ingest date is the same the move,
        # so they're all old
        assert len(self.test_finder.old_sms) == 3

    @pytest.mark.parametrize(*SMSFinder_BAD_PATHS)
    def test_fails_on_no_data(self, source):
        """Test that an error is raised if no files are found."""
        with pytest.raises(OSError):
            SMSFinder(source)

    def test_finds_ingested(self):
        """Test that if there is no table, ingested_sms is None."""
        assert len(self.test_finder.ingested_sms) == 3
