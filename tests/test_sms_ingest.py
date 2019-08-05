import pytest
import os

from cosmo.sms import SMSFinder, SMSFile, SMSFileStats, SMSTable, ingest_sms_data

TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/')


@pytest.fixture(params=[os.path.dirname(os.path.abspath(__file__)), '/this/is/not/a/directory'])
def bad_file_path(request):
    """Fixture that parametrizes cases of file paths that should result in an error."""
    bad_file_path = request.param

    return bad_file_path


@pytest.fixture(params=[os.path.join(TEST_DATA, test_file) for test_file in ['111078a6.txt', '180147b1.txt']])
def smsfile(request):
    """Fixture that parametrizes cases of two files (one old format and one new) that should be ingested
    successfully.

    Includes a clean up for tests that create tables in the test database.
    """
    filename = request.param
    file_id = os.path.basename(filename)[:6]
    version = os.path.basename(filename)[6:8]

    smsfile = (filename, file_id, version)
    yield smsfile

    if SMSTable.table_exists():
        SMSTable.drop_table()

    if SMSFileStats.table_exists():
        SMSFileStats.drop_table()


@pytest.fixture
def ingest_source():
    """Fixture that includes a clean up that removes tables that are created for database ingestion tests."""
    yield TEST_DATA

    SMSFileStats.drop_table()
    SMSTable.drop_table()


@pytest.fixture()
def test_finder():
    """Fixture that yields an SMSFinder object for testing. Clean up removes database tables."""
    test_finder = SMSFinder(TEST_DATA)
    yield test_finder

    if SMSTable.table_exists():
        SMSTable.drop_table()

    if SMSFileStats.table_exists():
        SMSFileStats.drop_table()


class TestIngestSmsData:
    """Test class that includes tests for ingesting SMS file data into the database."""

    def test_cold_start(self, ingest_source):
        """Test that ingest_sms_data executes successfully with cold_start enabled."""
        ingest_sms_data(ingest_source, cold_start=True)

        # Check that the tables were created
        assert SMSFileStats.table_exists() and SMSTable.table_exists()

        # Of the 5 data sets, only 3 should be ingested since SMSFinder only looks for the most recent version of the
        # files.
        assert len(SMSFileStats.select()) == 3

        # An error should be raised if a cold start is attempted with a populated database
        with pytest.raises(TypeError):
            ingest_sms_data(ingest_source, cold_start=True)


class TestSMSFile:
    """Test class that includes tests for the SMSFile object."""

    def test_data_ingest(self, smsfile):
        """Test that SMSFile is initialized successfully and that the file data is correctly found and ingested."""
        SMSFile(*smsfile)

    def test_ingest_fail(self):
        """Test that ingestion fails for a file with an unknown format."""
        bad_file = os.path.join(TEST_DATA, 'bad_111078a6.txt')

        with pytest.raises(ValueError):
            SMSFile(bad_file, '', '')

    def test_datatypes(self, smsfile):
        """Test that the ingested dtypes are correct for each field."""
        correct_dtypes = {
            'FILEID': object,
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

        sms = SMSFile(*smsfile)
        dtypes = sms.data.dtypes

        for key, value in dtypes.iteritems():
            assert value == correct_dtypes[key]

    def test_database_ingest(self, smsfile):
        """Test that the insert_to_db method executes successfully."""
        test_sms = SMSFile(*smsfile)
        test_sms.insert_to_db()


class TestSMSFinder:
    """Tests for SMSFinder"""

    def test_found(self, test_finder):
        """Test that sms files are found correctly."""
        assert len(test_finder.all_sms) == 3

    def test_sms_classification(self, test_finder):
        """Test that the sms files are correctly determined as new."""
        assert len(test_finder.new_sms) == 3  # All data is new if nothing is in the database
        assert test_finder.old_sms is None

        ingest_sms_data(TEST_DATA, cold_start=True)
        ingested_test_finder = SMSFinder(TEST_DATA)

        assert ingested_test_finder.new_sms is None  # All data was ingested
        assert len(ingested_test_finder.currently_ingested) == 3
        assert len(ingested_test_finder.old_sms) == 3

    def test_fails_on_no_data(self, bad_file_path):
        """Test that an error is raised if no files are found."""
        with pytest.raises(OSError):
            SMSFinder(bad_file_path)

    def test_version_filter(self, test_finder):
        """Test that the sms finder filters the files and only 'finds' the most recent version of the available SMS."""
        # test SMS file set, 181137 includes three versions of the same SMS.
        # The only file reported by SMSFinder should be the 'newest' version, c2.
        testcase = test_finder.new_sms[test_finder.new_sms.file_id == '181137']

        assert len(testcase) == 1
        assert testcase.version.values[0] == 'c2'
