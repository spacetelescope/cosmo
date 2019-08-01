import pytest
import os

from cosmo.sms import SMSFinder, SMSFile, SMSFileStats, SMSTable, ingest_sms_data

TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/')


@pytest.fixture(params=[os.path.dirname(os.path.abspath(__file__)), '/this/is/not/a/directory'])
def bad_file_path(request):
    bad_file_path = request.param

    return bad_file_path


@pytest.fixture(params=[os.path.join(TEST_DATA, test_file) for test_file in ['111078a6.txt', '180147b1.txt']])
def smsfile(request):
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
    yield TEST_DATA

    SMSFileStats.drop_table()
    SMSTable.drop_table()


@pytest.fixture()
def test_finder():
    test_finder = SMSFinder(TEST_DATA)
    yield test_finder

    if SMSTable.table_exists():
        SMSTable.drop_table()

    if SMSFileStats.table_exists():
        SMSFileStats.drop_table()


class TestIngestSmsData:

    def test_cold_start(self, ingest_source):
        ingest_sms_data(ingest_source, cold_start=True)

        # Check that the tables were created
        assert SMSFileStats.table_exists() and SMSTable.table_exists()


class TestSMSFile:

    def test_data_ingest(self, smsfile):
        SMSFile(*smsfile)

    def test_ingest_fail(self):
        bad_file = os.path.join(TEST_DATA, 'bad_111078a6.txt')

        with pytest.raises(ValueError):
            SMSFile(bad_file, '', '')

    def test_datatypes(self, smsfile):
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
        test_sms = SMSFile(*smsfile)
        test_sms.insert_to_db()


class TestSMSFinder:  # TODO: Add tests for finding new versions of SMS files
    """Tests for SMSFinder"""

    def test_found(self, test_finder):
        """Test that sms files are found correctly."""
        assert len(test_finder.all_sms) == 2

    def test_sms_classification(self, test_finder):
        """Test that the sms files are correctly determined as new."""
        test_finder = SMSFinder(TEST_DATA)

        assert len(test_finder.new_sms) == 2  # All data is new if nothing is in the database
        assert test_finder.old_sms is None

        ingest_sms_data(TEST_DATA, cold_start=True)
        ingested_test_finder = SMSFinder(TEST_DATA)

        assert ingested_test_finder.new_sms is None  # All data was ingested
        assert len(ingested_test_finder.currently_ingested) == 2
        assert len(ingested_test_finder.old_sms) == 2

    def test_fails_on_no_data(self, bad_file_path):
        """Test that an error is raised if no files are found."""
        with pytest.raises(OSError):
            SMSFinder(bad_file_path)
