"""Implementation of the FIRDS extractor tool."""

import csv
import xml.etree.ElementTree as ET
from io import BytesIO
from pathlib import Path
from typing import IO
from zipfile import ZipFile

import httpx
from pydantic import ValidationError
from tqdm import tqdm

from etl_processor.exceptions import NetworkError
from etl_processor.exceptions import ValidationError as ETLValidationError
from etl_processor.models import FIRDS, FIRDSDoc
from etl_processor.tool import Tool

FIRDS_NAMESPACE = '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}'


class FIRDSExtractor(Tool):
    """
    Extraction tool to gather financial instruments in the financial instrument reference data system (FIRDS).
    It starts by extracting DLTINS files from the FIRDS database by ESMA. Then, it parses the main attributes of the financial instruments returning a list of FIRDS documents.

    Attributes
    ----------
    firds_url : str
        The URL to the FIRDS database by ESMA.
    data_dir : str | Path
        The directory to save the extracted FIRDS documents.

    Examples
    --------
    >>> import asyncio
    >>> firds_extractor = FIRDSExtractor(
    ...     firds_url='https://example.com',
    ...     data_dir='data',
    ... )
    >>> async def main() -> None:
    ...     await firds_extractor.arun()
    >>> asyncio.run(main())
    """

    def __init__(
        self,
        firds_url: str,
        data_dir: str | Path,
    ) -> None:
        """
        Initialize the FIRDS extractor tool.

        Parameters
        ----------
        firds_url : str
            The URL to the FIRDS database by ESMA.
        data_dir : str | Path
            The directory to save the extracted FIRDS documents.
        """
        self.firds_url = firds_url

        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.firds_csv_path = self.data_dir / 'firds.csv'

    def _fetch_and_parse_firds_ref_doc(self) -> list[FIRDSDoc]:
        # get the firds reference doc from the firds_url
        try:
            # TODO: log the request
            firds_ref_doc_response = httpx.get(self.firds_url)
            firds_ref_doc_response.raise_for_status()

        except httpx.HTTPError as exc:
            # TODO: log the error
            raise NetworkError('Error fetching the FIRDS reference document.') from exc

        # iterate over the list of docs to get the firds zip
        firds_ref_doc_content = firds_ref_doc_response.text.encode('utf-8')
        firds_ref_doc_tree = ET.fromstring(firds_ref_doc_content)
        firds_ref_docs = []
        for ref_doc_element in firds_ref_doc_tree.iter('doc'):
            ref_doc_dict = {}

            for attr in ref_doc_element.findall('./'):
                attr_name = attr.attrib['name']
                ref_doc_dict[attr_name] = attr.text

            try:
                ref_doc = FIRDSDoc.model_validate(ref_doc_dict)

            except ValidationError as exc:
                # TODO: log the error
                raise ETLValidationError(str(exc)) from exc

            firds_ref_docs.append(ref_doc)

        # TODO: log the parsed firds reference doc
        return firds_ref_docs

    def _parse_firds_xml_file(self, firds_xml: IO[bytes]) -> None:
        with self.firds_csv_path.open('a', newline='', encoding='utf-8') as f:
            fieldnames = FIRDS.csv_header()
            writer = csv.DictWriter(f, fieldnames=fieldnames)

            # iterate over the xml file to get the financial instruments
            firds_zip_iterable = ET.iterparse(firds_xml, ('end',))
            for _, elem in firds_zip_iterable:
                # find the financial instrument tag
                elem_tag = elem.tag.replace(FIRDS_NAMESPACE, '')
                if 'FinInstrm' != elem_tag:
                    continue

                # parse the financial instrument attributes
                firds_dict = {}
                for attr in elem[0][0].findall('./'):
                    attr_tag = attr.tag.replace(FIRDS_NAMESPACE, '')
                    firds_dict[attr_tag] = attr.text

                # parse the financial instrument issuer
                firds_dict['Issr'] = elem[0][1].text

                # validate the financial instrument
                try:
                    firds = FIRDS.model_validate(firds_dict)

                except ValidationError:
                    # TODO: log the error
                    continue

                firds_validated_dict = firds.model_dump(by_alias=True)
                writer.writerow(firds_validated_dict)

        return

    def _parse_firds_zip_file(self, firds_zip_content: bytes) -> None:
        # zip files are light, so we can read them in memory
        # otherwise, we should stream the zip file in chunks and save it to disk to avoid memory issues.
        # note that the zip file itself is light but the xml files inside it can be heavy

        firds_zip_io = BytesIO(firds_zip_content)
        with ZipFile(firds_zip_io, 'r') as firds_zip:
            # find the xml file in the zip
            for firds_file_path in firds_zip.namelist():
                if not firds_file_path.endswith('.xml'):
                    continue

                # open xml file without extracting it
                with firds_zip.open(firds_file_path) as firds_xml:
                    return self._parse_firds_xml_file(firds_xml=firds_xml)

        return

    def _fetch_and_parse_firds_files(self, firds_ref_docs: list[FIRDSDoc]) -> None:
        # download the firds zip files
        for firds_ref_doc in firds_ref_docs:
            try:
                # TODO: log the request

                # fetch the firds zip file
                firds_zip_response = httpx.get(firds_ref_doc.download_link)
                firds_zip_response.raise_for_status()

            except httpx.HTTPError as exc:
                # TODO: log the error
                raise NetworkError('Error fetching the FIRDS zip file.') from exc

            # parse the firds zip file
            self._parse_firds_zip_file(firds_zip_response.content)

        return

    async def _afetch_and_parse_firds_files(self, firds_ref_docs: list[FIRDSDoc]) -> None:
        # async client to pool several requests to download the firds zip files
        async with httpx.AsyncClient() as client:
            for firds_ref_doc in tqdm(firds_ref_docs):
                try:
                    # TODO: log the request

                    # fetch the firds zip file
                    firds_zip_response = await client.get(firds_ref_doc.download_link)
                    firds_zip_response.raise_for_status()

                except httpx.HTTPError as exc:
                    # TODO: log the error
                    raise NetworkError('Error fetching the FIRDS zip file.') from exc

                # parse the firds zip file
                self._parse_firds_zip_file(firds_zip_response.content)

        return

    async def arun(self) -> None:
        """
        Extract data from the FIRDS database by ESMA asynchronously.

        Raises
        ------
        NetworkError
            If an error occurs during the extraction of the FIRDS document.
        ValidationError
            If an error occurs during the validation of the FIRDS document.
        """
        # get the firds reference docs
        firds_ref_docs = self._fetch_and_parse_firds_ref_doc()

        # write the csv header
        with self.firds_csv_path.open('w', newline='', encoding='utf-8') as f:
            fieldnames = FIRDS.csv_header()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

        # fetch the firds zip files
        await self._afetch_and_parse_firds_files(firds_ref_docs)

        return

    def run(self) -> None:
        """
        Extract data from the FIRDS database by ESMA.

        Raises
        ------
        NetworkError
            If an error occurs during the extraction of the FIRDS document.
        """
        # get the firds reference docs
        firds_ref_docs = self._fetch_and_parse_firds_ref_doc()

        # write the csv header
        with self.firds_csv_path.open('w', newline='', encoding='utf-8') as f:
            fieldnames = FIRDS.csv_header()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

        # fetch the firds zip files
        self._fetch_and_parse_firds_files(firds_ref_docs)

        return


if __name__ == '__main__':
    import doctest

    doctest.testmod()
