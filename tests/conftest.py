import os
import pytest

from glob import glob

HERE = os.path.dirname(os.path.abspath(__file__))

TEST_CONFIG = os.path.join(HERE, 'cosmoconfig_test.yaml')

# Check to make sure that the test config file is being used. If not, don't run the tests
if os.environ['MONITOR_CONFIG'] != TEST_CONFIG:
    raise TypeError('Tests should only be executed with the testing configuration file')

if os.environ['COSMO_SMS_DB'] != os.path.join(HERE, 'test.db'):
    raise TypeError('Test should only be executed with a test database')


@pytest.fixture(scope='session', autouse=True)
def db_cleanup():
    yield  # The tests don't actually need this test "value"

    # Cleanup
    if os.path.exists('test.db'):
        os.remove('test.db')   # Delete test database file after the completion of all tests

    # Remove temporary shared memory file if it exists
    if os.path.exists('test.db-shm'):
        os.remove('test.db-shm')

    # Remove temporary write-ahead log file if it exists
    if os.path.exists('test.db-wal'):
        os.remove('test.db-wal')

    if os.path.exists('data_test.db'):
        os.remove('data_test.db')

    if os.path.exists('data_test.db-shm'):
        os.remove('data_test.db-shm')

    if os.path.exists('data_test.db-wal'):
        os.remove('data_test.db-wal')


@pytest.fixture(scope='session')
def data_dir():
    test_data = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/')
    os.environ['CRDS_PATH'] = os.path.join(test_data, 'test_crds_cache')

    return test_data


@pytest.fixture(scope='session')
def here():
    return HERE


@pytest.fixture(scope='session', autouse=True)
def clean_up_output(here):
    yield

    output = glob(os.path.join(here, '*html')) + glob(os.path.join(here, '*csv'))

    if output:
        for file in output:
            os.remove(file)
