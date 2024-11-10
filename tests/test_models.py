from datetime import datetime

import pytest
from pydantic import ValidationError

from etl_processor.models import FIRDS, FIRDSDoc


def test_create_firds_doc(firds_doc_data: dict[str, str]) -> None:
    """
    Test create FIRDSDoc.
    """
    firds_doc = FIRDSDoc.model_validate(firds_doc_data)
    assert firds_doc.id == firds_doc_data['id']
    assert firds_doc.checksum == firds_doc_data['checksum']
    assert firds_doc.download_link == firds_doc_data['download_link']
    assert firds_doc.publication_date == datetime.fromisoformat(firds_doc_data['publication_date'])


def test_create_firds_doc_missing_fields(firds_doc_data: dict[str, str]) -> None:
    """
    Test fail to create FIRDSDoc with missing fields.
    """
    firds_doc_data.pop('id')
    with pytest.raises(ValidationError):
        FIRDSDoc.model_validate(firds_doc_data)


def test_create_firds_doc_invalid_data(firds_doc_data: dict[str, str]) -> None:
    """
    Test fail to create FIRDSDoc with invalid data.
    """
    firds_doc_data['publication_date'] = 'abc'
    with pytest.raises(ValidationError):
        FIRDSDoc.model_validate(firds_doc_data)


def test_create_firds(firds: dict[str, str]) -> None:
    """
    Test create FIRDS.
    """
    firds_model = FIRDS.model_validate(firds)
    assert firds_model.id == firds['Id']
    assert firds_model.full_name == firds['FullNm']
    assert firds_model.instrument_type == firds['ClssfctnTp']
    
    if firds['CmmdtyDerivInd'].lower() == 'true':
        firds_commodity_derivative_indicator = True
    else:
        firds_commodity_derivative_indicator = False
    assert firds_model.commodity_derivative_indicator == firds_commodity_derivative_indicator
    
    assert firds_model.notional_currency == firds['NtnlCcy']
    assert firds_model.issuer == firds['Issr']


def test_create_firds_missing_fields(firds: dict[str, str]) -> None:
    """
    Test fail to create FIRDS with missing fields.
    """
    firds.pop('Id')
    with pytest.raises(ValidationError):
        FIRDS.model_validate(firds)


def test_create_firds_invalid_data(firds: dict[str, str]) -> None:
    """
    Test fail to create FIRDS with invalid data.
    """
    firds['CmmdtyDerivInd'] = 'abc'
    with pytest.raises(ValidationError):
        FIRDS.model_validate(firds)


def test_firds_csv_header() -> None:
    """
    Test the csv_header class method of FIRDS.
    """
    expected_header = [
        'FinInstrmGnlAttrbts.Id',
        'FinInstrmGnlAttrbts.FullNm',
        'FinInstrmGnlAttrbts.ClssfctnTp',
        'FinInstrmGnlAttrbts.CmmdtyDerivInd',
        'FinInstrmGnlAttrbts.NtnlCcy',
        'Issr',
    ]
    assert FIRDS.csv_header() == expected_header
