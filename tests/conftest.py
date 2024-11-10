import pytest


@pytest.fixture
def firds_doc_data() -> dict[str, str]:
    """
    Fixture for creating a FIRDSDoc instance with valid data.
    """
    return {
        'id': '46015',
        'checksum': '852b2dde71cf114289ad95ada2a4e406',
        'download_link': 'https://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip',
        'publication_date': '2021-01-17T00:00:00Z',
        '_root_': '46015',
        'published_instrument_file_id': '46015',
        'file_name': 'DLTINS_20210117_01of01.zip',
        'file_type': 'DLTINS',
        '_version_': '1815300667920613400',
        'timestamp': '2024-11-10T02:27:03.551Z',
    }


@pytest.fixture
def firds() -> dict[str, str]:
    """
    Fixture for creating a FIRDS instance with valid data.
    """
    return {
        'Id': 'DE000A1R07V3',
        'FullNm': 'Kreditanst.f.Wiederaufbau     Anl.v.2014 (2021)',
        'ClssfctnTp': 'DBFTFB',
        'CmmdtyDerivInd': 'false',
        'NtnlCcy': 'EUR',
        'Issr': '549300GDPG70E3MBBU98',
    }
