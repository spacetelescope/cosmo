import os
import pytest

TEST_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cosmoconfig_test.yaml')

# Check to make sure that the test config file is being used. If not, don't run the tests
if os.environ['COSMO_CONFIG'] != TEST_CONFIG:
    raise TypeError('Tests should only be executed with the testing configuration file')


@pytest.fixture(scope='session', autouse=True)
def db_cleanup():
    yield  # The tests don't actually need this test "value"

    # Cleanup
    os.remove('test.db')   # Delete test database file after the completion of all tests

    # Remove temporary shared memory file if it exists
    if os.path.exists('test.db-shm'):
        os.remove('test.db-shm')

    # Remove temporary write-ahead log file if it exists
    if os.path.exists('test.db-wal'):
        os.remove('test.db-wal')
