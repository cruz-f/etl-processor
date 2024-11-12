from pathlib import Path

import pytest


@pytest.mark.transform
def test_init(firds_csv: Path) -> None:
    """
    Test FIRDSTransformer init.
    """
    from etl_processor.transform import FIRDSTransformer

    firds_transformer = FIRDSTransformer(
        data_dir=firds_csv.parent,
    )
    assert firds_transformer.data_dir == firds_csv.parent
    assert firds_transformer.chunk_size == 10**6
    assert firds_transformer.firds_csv_path == firds_csv


@pytest.mark.transform
def test_run(firds_csv: Path) -> None:
    """
    Test FIRDSTransformer run.
    """
    from etl_processor.transform import FIRDSTransformer

    firds_transformer = FIRDSTransformer(
        data_dir=firds_csv.parent,
    )
    firds_transformer.run()

    firds_transform_csv = firds_csv.parent / 'firds_transformed.csv'

    assert firds_transform_csv.exists()
    assert firds_transform_csv.stat().st_size > 0

    # check the 'a_count' and 'contains_a' columns exist
    with firds_transform_csv.open() as f:
        header = f.readline().strip().split(',')
        assert 'a_count' in header
        assert 'contains_a' in header
        return


@pytest.mark.transform
@pytest.mark.asyncio
async def test_arun(firds_csv: Path) -> None:
    """
    Test FIRDSTransformer arun.
    """
    from etl_processor.transform import FIRDSTransformer

    firds_transformer = FIRDSTransformer(
        data_dir=firds_csv.parent,
    )
    await firds_transformer.arun()

    firds_transform_csv = firds_csv.parent / 'firds_transformed.csv'

    assert firds_transform_csv.exists()
    assert firds_transform_csv.stat().st_size > 0

    # check the 'a_count' and 'contains_a' columns exist
    with firds_transform_csv.open() as f:
        header = f.readline().strip().split(',')
        assert 'a_count' in header
        assert 'contains_a' in header
        return
