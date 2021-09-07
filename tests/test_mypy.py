import pytest
import subprocess


@pytest.mark.extra
def test_mypy():
    """ Test the code with mypy """
    res = subprocess.run('mypy', shell=True)
    if res.returncode != 0:
        raise AssertionError('MyPy linting failed')
