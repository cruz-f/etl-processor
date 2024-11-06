import pytest

from etl_processor import __version__


@pytest.mark.chore
def test_version() -> None:
    """
    Test that the version is as expected.
    """
    assert isinstance(__version__, str)
