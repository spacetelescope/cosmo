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

SMSFile_GOOD_DATA =(
    ('file',),
    [
        (os.path.join(TEST_DATA, test_file),) for test_file in ['111078a6.txt', '180147b1.txt', '180148a5.l-exp']
    ]
)


class TestSMSFinder:

    @classmethod
    def setup_class(cls):
        test_time = datetime.datetime(2019, 6, 17, 12, 14, 23, 115663)

        # Initialize a test SMSFinder that always uses the same "today"
        cls.test_finder = SMSFinder(TEST_DATA)
        cls.test_finder.today = test_time  # set to the test "today"
        cls.test_finder._all_sms_results = cls.test_finder.find_all()  # Find the files and determine which are new

    def test_found(self):
        assert len(self.test_finder.all_sms) == 3

    def test_is_new(self):
        # test files were all moved on the same day as the "test" day so they're all "new"
        assert len(self.test_finder.new_sms) == 3

    @pytest.mark.parametrize(*SMSFinder_BAD_PATHS)
    def test_fails_on_no_data(self, source):
        with pytest.raises(OSError):
            SMSFinder(source)


class TestSMSFile:

    @pytest.mark.parametrize(*SMSFile_GOOD_DATA)
    def test_ingest(self, file):
        SMSFile(file)
