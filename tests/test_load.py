from pathlib import Path

import pytest


@pytest.mark.load
def test_init(firds_csv: Path) -> None:
    """
    Test FIRDSLoder init.
    """
    from etl_processor.load import FIRDSLoader

    firds_loader = FIRDSLoader(
        data_dir=firds_csv.parent,
        system='file',
        target_path=str(firds_csv.parent / 'firds_gold.csv'),
    )
    assert firds_loader.data_dir == firds_csv.parent
    assert firds_loader.system == 'file'
    assert firds_loader.target_path == str(firds_csv.parent / 'firds_gold.csv')
    assert firds_loader.storage_options == {}
    return


@pytest.mark.load
def test_run(firds_transformed_csv: Path) -> None:
    """
    Test FIRDSLoder run.
    """
    from etl_processor.load import FIRDSLoader

    firds_loader = FIRDSLoader(
        data_dir=firds_transformed_csv.parent,
        system='file',
        target_path=str(firds_transformed_csv.parent / 'firds_gold.csv'),
    )
    firds_loader.run()

    firds_gold_csv = firds_transformed_csv.parent / 'firds_gold.csv'

    assert firds_gold_csv.exists()
    assert firds_gold_csv.stat().st_size > 0
    return


@pytest.mark.load
@pytest.mark.asyncio
async def test_arun(firds_transformed_csv: Path) -> None:
    """
    Test FIRDSLoder arun.
    """
    from etl_processor.load import FIRDSLoader

    firds_loader = FIRDSLoader(
        data_dir=firds_transformed_csv.parent,
        system='file',
        target_path=str(firds_transformed_csv.parent / 'firds_gold.csv'),
    )
    await firds_loader.arun()

    firds_gold_csv = firds_transformed_csv.parent / 'firds_gold.csv'

    assert firds_gold_csv.exists()
    assert firds_gold_csv.stat().st_size > 0

    # check the 'a_count' and 'contains_a' columns exist
    with firds_gold_csv.open() as f:
        header = f.readline().strip().split(',')
        assert 'a_count' in header
        assert 'contains_a' in header
    return
