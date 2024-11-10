"""It contains the models for the ETL processor."""

from typing import Literal

from pydantic import AwareDatetime, BaseModel, Field


class FIRDSDoc(BaseModel):
    """
    Model for a financial instrument reference data system (FIRDS) reference document.
    It should contain attributes referring to a document available in the FIRDS database by ESMA.
    """

    id: str = Field(
        ...,
        description='Document unique identifier.',
    )
    checksum: str = Field(
        ...,
        description='Document checksum.',
    )
    download_link: str = Field(
        ...,
        description='Document download link.',
    )
    publication_date: AwareDatetime = Field(
        ...,
        description='Document publication date.',
    )
    root: str = Field(
        ...,
        validation_alias='_root_',
        description='Document root.',
    )
    published_instrument_file_id: str = Field(
        ...,
        description='Published instrument file unique identifier.',
    )
    file_name: str = Field(
        ...,
        description='Document file name.',
    )
    file_type: Literal['DLTINS'] = Field(
        ...,
        description='Document file type.',
    )
    version: int = Field(
        ...,
        validation_alias='_version_',
        description='Document version.',
    )
    timestamp: AwareDatetime = Field(
        ...,
        description='Document timestamp.',
    )


class FIRDS(BaseModel):
    """
    Model for a financial instrument in the financial instrument reference data system (FIRDS).
    It should contain attributes and metadata to all financial instruments.
    Follow the fields descriptions for more details.
    """

    id: str = Field(
        ...,
        validation_alias='Id',
        serialization_alias='FinInstrmGnlAttrbts.Id',
        description='Financial instrument unique identifier.',
    )
    full_name: str = Field(
        ...,
        validation_alias='FullNm',
        serialization_alias='FinInstrmGnlAttrbts.FullNm',
        description='Financial instrument full name.',
    )
    instrument_type: str = Field(
        ...,
        validation_alias='ClssfctnTp',
        serialization_alias='FinInstrmGnlAttrbts.ClssfctnTp',
        description='Financial instrument classification type.',
    )
    commodity_derivative_indicator: bool = Field(
        ...,
        validation_alias='CmmdtyDerivInd',
        serialization_alias='FinInstrmGnlAttrbts.CmmdtyDerivInd',
        description='Financial instrument commodity derivative indicator.',
    )
    notional_currency: str = Field(
        ...,
        validation_alias='NtnlCcy',
        serialization_alias='FinInstrmGnlAttrbts.NtnlCcy',
        description='Financial instrument notional currency.',
    )
    issuer: str = Field(
        ...,
        alias='Issr',
        description='Issuer unique identifier.',
    )

    @classmethod
    def csv_header(cls) -> list[str]:
        """
        Return the CSV header for the model.

        Returns
        -------
        list[str]
            The CSV header for the model.
        """
        schema = cls.model_json_schema(by_alias=True)
        properties = schema['properties']
        columns = []
        for key in properties.keys():
            if key == 'Issr':
                columns.append('Issr')
            else:
                columns.append(f'FinInstrmGnlAttrbts.{key}')

        return columns
