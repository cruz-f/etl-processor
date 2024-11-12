from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from etl_processor.extract import FIRDSExtractor
    from etl_processor.models import FIRDS, FIRDSDoc


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
def firds_doc(firds_doc_data: dict[str, str]) -> 'FIRDSDoc':
    """
    Fixture for creating a FIRDSDoc instance with valid data.
    """
    from etl_processor.models import FIRDSDoc

    return FIRDSDoc.model_validate(firds_doc_data)


@pytest.fixture
def firds_data() -> dict[str, str]:
    """
    Fixture for creating a FIRDS instance with valid data.
    """
    return {
        'Id': 'EZV1JDJ1R5Q9',
        'FullNm': 'Foreign_Exchange Forward JPY SEK 20210116',
        'ClssfctnTp': 'JFTXFP',
        'CmmdtyDerivInd': 'false',
        'NtnlCcy': 'SEK',
        'Issr': '2138004TYNQCB7MLTG76',
    }


@pytest.fixture
def firds(firds_data: dict[str, str]) -> 'FIRDS':
    """
    Fixture for creating a FIRDS instance with valid data.
    """
    from etl_processor.models import FIRDS

    return FIRDS.model_validate(firds_data)


@pytest.fixture
def firds_extractor() -> 'FIRDSExtractor':
    """
    Fixture for creating a FIRDSExtractor instance.
    """
    from etl_processor.extract import FIRDSExtractor

    return FIRDSExtractor(firds_url='https://example.com', data_dir='data')


@pytest.fixture
def firds_ref_doc_response() -> str:
    """
    Fixture of a FIRDS reference document XML.
    """
    return """
<doc>
    <str name="checksum">852b2dde71cf114289ad95ada2a4e406</str>
    <str name="download_link">https://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip</str>
    <date name="publication_date">2021-01-17T00:00:00Z</date>
    <str name="id">46015</str>
    <str name="_root_">46015</str>
    <str name="published_instrument_file_id">46015</str>
    <str name="file_name">DLTINS_20210117_01of01.zip</str>
    <str name="file_type">DLTINS</str>
    <long name="_version_">1815300667920613400</long>
    <date name="timestamp">2024-11-10T02:27:03.551Z</date>
</doc>
"""


@pytest.fixture
def firds_xml_data() -> str:
    """
    Fixture of a FIRDS XML document.
    """
    return """
<BizData xsi:schemaLocation="urn:iso:std:iso:20022:tech:xsd:head.003.001.01 head.003.001.01.xsd" xmlns="urn:iso:std:iso:20022:tech:xsd:head.003.001.01" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Hdr><AppHdr xsi:schemaLocation="urn:iso:std:iso:20022:tech:xsd:head.001.001.01 head.001.001.01_ESMAUG_1.0.0.xsd" xmlns="urn:iso:std:iso:20022:tech:xsd:head.001.001.01" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Fr><OrgId><Id><OrgId><Othr><Id>EU</Id></Othr></OrgId></Id></OrgId></Fr><To><OrgId><Id><OrgId><Othr><Id>Public</Id></Othr></OrgId></Id></OrgId></To><BizMsgIdr>DLTINS_20210118_01of01</BizMsgIdr><MsgDefIdr>auth.036.001.02</MsgDefIdr><CreDt>2021-01-18T01:21:23.773Z</CreDt></AppHdr></Hdr><Pyld><Document xsi:schemaLocation="urn:iso:std:iso:20022:tech:xsd:auth.036.001.02 auth.036.001.02_ESMAUG_DLTINS_1.1.0.xsd" xmlns="urn:iso:std:iso:20022:tech:xsd:auth.036.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><FinInstrmRptgRefDataDltaRpt><RptHdr><RptgNtty><NtlCmptntAuthrty>EU</NtlCmptntAuthrty></RptgNtty><RptgPrd><Dt>2021-01-18</Dt></RptgPrd></RptHdr><FinInstrm><TermntdRcrd><FinInstrmGnlAttrbts><Id>EZV1JDJ1R5Q9</Id><FullNm>Foreign_Exchange Forward JPY SEK 20210116</FullNm><ClssfctnTp>JFTXFP</ClssfctnTp><NtnlCcy>SEK</NtnlCcy><CmmdtyDerivInd>false</CmmdtyDerivInd></FinInstrmGnlAttrbts><Issr>2138004TYNQCB7MLTG76</Issr><TradgVnRltdAttrbts><Id>EBSF</Id><IssrReq>true</IssrReq><AdmssnApprvlDtByIssr>2018-01-30T00:23:09.561Z</AdmssnApprvlDtByIssr><ReqForAdmssnDt>2018-01-30T00:23:09.705Z</ReqForAdmssnDt><FrstTradDt>2018-01-30T00:23:09.705Z</FrstTradDt><TermntnDt>2021-01-16T23:59:59.999Z</TermntnDt></TradgVnRltdAttrbts><DerivInstrmAttrbts><XpryDt>2021-01-16</XpryDt><PricMltplr>1</PricMltplr><DlvryTp>PHYS</DlvryTp><AsstClssSpcfcAttrbts><FX><FxTp>FXMJ</FxTp><OthrNtnlCcy>JPY</OthrNtnlCcy></FX></AsstClssSpcfcAttrbts></DerivInstrmAttrbts><TechAttrbts><RlvntCmptntAuthrty>NL</RlvntCmptntAuthrty><PblctnPrd><FrDtToDt><FrDt>2021-01-03</FrDt><ToDt>2021-01-17</ToDt></FrDtToDt></PblctnPrd><RlvntTradgVn>EBSF</RlvntTradgVn></TechAttrbts></TermntdRcrd></FinInstrm></FinInstrmRptgRefDataDltaRpt></Document></Pyld></BizData>
"""


@pytest.fixture(scope='session')
def firds_csv(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """
    Fixture of a FIRDS CSV document.
    It creates a temporary file mocking the FIRDS CSV data.
    """
    firds_csv_data = """
FinInstrmGnlAttrbts.Id,FinInstrmGnlAttrbts.FullNm,FinInstrmGnlAttrbts.ClssfctnTp,FinInstrmGnlAttrbts.CmmdtyDerivInd,FinInstrmGnlAttrbts.NtnlCcy,Issr
DE000A1R07V3,Kreditanst.f.Wiederaufbau     Anl.v.2014 (2021),DBFTFB,False,EUR,549300GDPG70E3MBBU98
DE000A1R07V3,KFW 1 5/8 01/15/21,DBFTFB,False,EUR,549300GDPG70E3MBBU98
DE000A1R07V3,Kreditanst.f.Wiederaufbau Anl.v.2014 (2021),DBFTFB,False,EUR,549300GDPG70E3MBBU98
DE000A1R07V3,Kreditanst.f.Wiederaufbau Anl.v.2014 (2021),DBFTFB,False,EUR,549300GDPG70E3MBBU98
"""
    firds_csv_path = tmp_path_factory.mktemp('data') / 'firds.csv'
    firds_csv_path.write_text(firds_csv_data)
    return firds_csv_path
