import pytest
import os
import datetime

from cosmo.sms import SMSFinder, SMSFile

# TODO: Set up a test database to use during these tests
TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/')

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


class TestSMSFinder:
    """Tests for SMSFinder"""

    @classmethod
    def setup_class(cls):
        """Set up the tests with a 'test' instance of the SMSFinder."""
        test_time = datetime.datetime(2019, 6, 17, 12, 14, 23, 115663)

        # Initialize a test SMSFinder that always uses the same "today"
        cls.test_finder = SMSFinder(TEST_DATA)
        cls.test_finder.today = test_time  # set to the test "today"
        cls.test_finder.last_ingest_date = test_time  # set the last ingest to test "today"
        cls.test_finder._all_sms_results = cls.test_finder.find_all()  # Find the files and determine which are new

    def test_found(self):
        """Test that sms files are found correctly."""
        assert len(self.test_finder.all_sms) == 3

    def test_new_sms(self):
        """Test that the sms files are correctly determined as new."""
        # test files were all moved on the same day as the "test" day and the last_ingest date is the same the move,
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

    def test_no_dbtable(self):
        """Test that if there is no table, ingested_sms is None."""
        assert self.test_finder.ingested_sms is None


class TestSMSFile:

    @pytest.mark.parametrize(*SMSFile_GOOD_DATA)
    def test_ingest(self, file):
        SMSFile(file)

    def test_ingest_fail(self):
        bad_file = os.path.join(TEST_DATA, 'bad_111078a6.txt')

        with pytest.raises(ValueError):
            SMSFile(bad_file)

    @pytest.mark.parametrize(*SMSFile_GOOD_DATA)
    def test_datatypes(self, file):
        correct_dtypes = {
            'filename': object,
            'rootname': object,
            'proposid': int,
            'detector': object,
            'opmode': object,
            'exptime': float,
            'expstart': object,
            'fuvhvstate': object,
            'aperture': object,
            'osm1pos': object,
            'osm2pos': object,
            'cenwave': int,
            'fppos': int,
            'tsinceosm1': float,
            'tsinceosm2': float
        }

        sms = SMSFile(file)
        dtypes = sms.data.dtypes

        for key, value in dtypes.iteritems():
            assert value == correct_dtypes[key]
