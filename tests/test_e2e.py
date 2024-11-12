from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@patch('etl_processor.extract.httpx.get')
@pytest.mark.e2e
@pytest.mark.asyncio
async def test_etl(
    mock_get: MagicMock,
    firds_ref_doc_response: str,
    firds_csv: Path,
) -> None:
    """
    Test the ETL process.
    """
    mock_response = MagicMock()
    mock_response.text = firds_ref_doc_response
    mock_get.return_value = mock_response

    from etl_processor.extract import FIRDSExtractor
    from etl_processor.load import FIRDSLoader
    from etl_processor.transform import FIRDSTransformer

    firds_extractor = FIRDSExtractor(
        firds_url='https://mock-url.mock.domain',
        data_dir=firds_csv.parent,
    )
    await firds_extractor.arun()

    firds_transformer = FIRDSTransformer(
        data_dir=firds_csv.parent,
    )
    await firds_transformer.arun()

    firds_loader = FIRDSLoader(
        data_dir=firds_csv.parent,
        system='file',
        target_path=str(firds_csv.parent / 'firds_gold.csv'),
    )
    await firds_loader.arun()

    firds_gold_csv = firds_csv.parent / 'firds_gold.csv'

    assert firds_gold_csv.exists()
    assert firds_gold_csv.stat().st_size > 0
    return
