from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

if TYPE_CHECKING:
    from etl_processor.extract import FIRDSExtractor
    from etl_processor.models import FIRDS, FIRDSDoc


@pytest.mark.extract
def test_init() -> None:
    """
    Test FIRDSExtractor init.
    """
    from etl_processor.extract import FIRDSExtractor

    firds_extractor = FIRDSExtractor(firds_url='https://example.com', data_dir='data')
    assert firds_extractor.firds_url == 'https://example.com'
    assert firds_extractor.data_dir == Path('data')
    assert firds_extractor.data_dir.exists()
    assert firds_extractor.firds_csv_path == Path('data/firds.csv')


@patch('etl_processor.extract.httpx.get')
@pytest.mark.extract
def test_fetch_and_parse_firds_ref_doc(
    mock_get: MagicMock,
    firds_extractor: 'FIRDSExtractor',
    firds_doc: 'FIRDSDoc',
    firds_ref_doc_response: str,
) -> None:
    """
    Test _fetch_and_parse_firds_ref_doc method.
    """
    mock_response = MagicMock()
    mock_response.text = firds_ref_doc_response
    mock_get.return_value = mock_response

    # TODO: consider patching the ET parse methods in the future
    result = firds_extractor._fetch_and_parse_firds_ref_doc()
    assert len(result) == 1
    assert result[0].download_link == firds_doc.download_link


@patch('etl_processor.extract.csv.DictWriter')
@pytest.mark.extract
def test_parse_firds_xml_file(
    mock_dict_writer: MagicMock,
    firds_extractor: 'FIRDSExtractor',
    firds: 'FIRDS',
    firds_xml_data: str,
) -> None:
    """
    Test _parse_firds_xml_file method.
    """
    from io import BytesIO

    firds_xml = BytesIO(firds_xml_data.encode('utf-8'))

    with patch.object(firds_extractor, 'firds_csv_path') as csv_path:
        csv_path.open = MagicMock()

        firds_extractor._parse_firds_xml_file(firds_xml=firds_xml)

        csv_path.open.assert_called_once_with('a', newline='', encoding='utf-8')

        mock_dict_writer.assert_called_once_with(
            csv_path.open.return_value.__enter__.return_value,
            fieldnames=firds.csv_header(),
        )

        mock_dict_writer.return_value.writerow.assert_called_once_with(firds.model_dump(by_alias=True))


@patch('etl_processor.extract.ZipFile')
@patch('etl_processor.extract.BytesIO')
@pytest.mark.extract
def test_parse_firds_zip_file(
    mock_bytes_io: MagicMock,
    mock_zip: MagicMock,
    firds_extractor: 'FIRDSExtractor',
) -> None:
    """
    Test _parse_firds_zip_file method.
    """
    mock_zip_instance = MagicMock()
    mock_zip.return_value.__enter__.return_value = mock_zip_instance

    mock_zip_instance.namelist.return_value = ['file.xml', 'file.txt']

    mock_xml_file = MagicMock()
    mock_zip_instance.open.return_value = mock_xml_file

    with patch.object(firds_extractor, '_parse_firds_xml_file') as mock_parse:
        firds_extractor._parse_firds_zip_file(b'zip content')

        mock_bytes_io.assert_called_once_with(b'zip content')

        mock_zip.assert_called_once_with(mock_bytes_io.return_value, 'r')

        mock_zip_instance.namelist.assert_called_once()
        mock_zip_instance.open.assert_called_once_with('file.xml')

        mock_parse.assert_called_once_with(firds_xml=mock_zip_instance.open.return_value.__enter__.return_value)


@patch('etl_processor.extract.httpx.get')
@pytest.mark.extract
def test_fetch_and_parse_firds_files(
    mock_get: MagicMock,
    firds_extractor: 'FIRDSExtractor',
    firds_doc: 'FIRDSDoc',
) -> None:
    """
    Test _fetch_and_parse_firds_files method.
    """
    mock_response = MagicMock()
    mock_response.content = b'zip content'
    mock_get.return_value = mock_response

    firds_ref_docs = [firds_doc]
    with patch.object(firds_extractor, '_parse_firds_zip_file') as mock_parse:
        firds_extractor._fetch_and_parse_firds_files(firds_ref_docs)
        mock_parse.assert_called_once_with(b'zip content')


@patch('etl_processor.extract.httpx.AsyncClient.get')
@pytest.mark.asyncio
@pytest.mark.extract
async def test_afetch_and_parse_firds_files(
    mock_get: MagicMock,
    firds_extractor: 'FIRDSExtractor',
    firds_doc: 'FIRDSDoc',
) -> None:
    mock_response = MagicMock()
    mock_response.content = b'zip content'
    mock_get.return_value = mock_response

    firds_ref_docs = [firds_doc]
    with patch.object(firds_extractor, '_parse_firds_zip_file') as mock_parse:
        await firds_extractor._afetch_and_parse_firds_files(firds_ref_docs)
        mock_parse.assert_called_once_with(b'zip content')


@patch('etl_processor.extract.csv.DictWriter')
@pytest.mark.extract
def test_run(
    mock_dict_writer: MagicMock,
    firds_extractor: 'FIRDSExtractor',
    firds_doc: 'FIRDSDoc',
    firds: 'FIRDS',
) -> None:
    """
    Test run method.
    """
    with patch.object(firds_extractor, '_fetch_and_parse_firds_ref_doc') as mock_fetch:
        with patch.object(firds_extractor, '_fetch_and_parse_firds_files') as mock_parse:
            with patch.object(firds_extractor, 'firds_csv_path') as csv_path:
                csv_path.open = MagicMock()

                mock_fetch.return_value = [firds_doc]

                firds_extractor.run()

                csv_path.open.assert_called_once_with('w', newline='', encoding='utf-8')

                mock_dict_writer.assert_called_once_with(
                    csv_path.open.return_value.__enter__.return_value,
                    fieldnames=firds.csv_header(),
                )

                mock_dict_writer.return_value.writeheader.assert_called_once()

                mock_fetch.assert_called_once()
                mock_parse.assert_called_once_with([firds_doc])


@patch('etl_processor.extract.csv.DictWriter')
@pytest.mark.asyncio
@pytest.mark.extract
async def test_arun(
    mock_dict_writer: MagicMock,
    firds_extractor: 'FIRDSExtractor',
    firds_doc: 'FIRDSDoc',
    firds: 'FIRDS',
) -> None:
    """
    Test arun method.
    """
    with patch.object(firds_extractor, '_fetch_and_parse_firds_ref_doc') as mock_fetch:
        with patch.object(firds_extractor, '_afetch_and_parse_firds_files') as mock_parse:
            with patch.object(firds_extractor, 'firds_csv_path') as csv_path:
                csv_path.open = MagicMock()

                mock_fetch.return_value = [firds_doc]

                await firds_extractor.arun()

                csv_path.open.assert_called_once_with('w', newline='', encoding='utf-8')

                mock_dict_writer.assert_called_once_with(
                    csv_path.open.return_value.__enter__.return_value,
                    fieldnames=firds.csv_header(),
                )

                mock_dict_writer.return_value.writeheader.assert_called_once()

                mock_fetch.assert_called_once()
                mock_parse.assert_called_once_with([firds_doc])
