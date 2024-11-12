"""Implementation of the FIRDS loader tool."""

from pathlib import Path
from typing import Any

import fsspec  # type: ignore
import pandas as pd

from etl_processor.exceptions import LoadError
from etl_processor.logger import logger
from etl_processor.tool import Tool


class FIRDSLoader(Tool):
    """
    Loading tool to save the FIRDS CSV into a file storage system.

    Attributes
    ----------
    data_dir : str | Path
        The directory to read the extracted FIRDS documents.
    system : str
        The file storage system to save the FIRDS data.
    target_path : str
        The path to save the FIRDS data in the file storage system.
    storage_options : dict[str, Any], optional
        The options to pass to the file storage system.

    Examples
    --------
    >>> storage_options = {
    ...     'anon': False,
    ... }
    >>> firds_loader = FIRDSLoader(
    ...     data_dir='data',
    ...     system='s3',
    ...     target_path='s3://my-bucket/firds_gold.csv',
    ...     storage_options=storage_options,
    ... )

    """

    def __init__(
        self,
        data_dir: str | Path,
        system: str,
        target_path: str,
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize the FIRDS loader tool.

        Parameters
        ----------
        data_dir : str | Path
            The directory to read the extracted FIRDS documents.
        system : str
            The file storage system to save the FIRDS data.
        target_path : str
            The path to save the FIRDS data in the file storage system.
        storage_options : dict[str, Any], optional
            The options to pass to the file storage system.
        """
        if storage_options is None:
            storage_options = {}

        fs = fsspec.filesystem(system, **storage_options)

        self.data_dir = Path(data_dir)
        self.system = system
        self.fs = fs
        self.target_path = target_path
        self.storage_options = storage_options
        self.firds_csv_path = self.data_dir / 'firds_transformed.csv'

    def run(self) -> None:
        """
        Load the FIRDS data.
        It reads the FIRDS CSV file and writes it to the file storage system.
        It uses the fsspec library to interact with the file storage system.

        Raises
        ------
        LoadError
            If an error occurs during the loading of the FIRDS data.
        """
        try:
            logger.info(f'Loading the FIRDS data in {self.firds_csv_path} to {self.target_path}')

            # read the firds csv file
            # TODO: read the csv file adds an extra validation step. It verifies the file is a valid csv file. However, it might not be necessary. We could well load the file directly to the file storage system.
            df = pd.read_csv(self.firds_csv_path)

            # write the dataframe to the file storage system
            with self.fs.open(self.target_path, 'wb') as f:
                df.to_csv(f, index=False)

            logger.info(f'The FIRDS data has been loaded to {self.target_path}')

        except Exception as exc:
            logger.error(f'Error loading the FIRDS data to {self.target_path}')
            raise LoadError('Error loading the FIRDS data.') from exc

        return

    async def arun(self) -> None:
        """
        Load the FIRDS data. Asynchronous version.
        It reads the FIRDS CSV file and writes it to the file storage system.
        It uses the fsspec library to interact with the file storage system.

        Raises
        ------
        LoadError
            If an error occurs during the loading of the FIRDS data.
        """
        # TODO: consider using the async version of fsspec to write the csv file
        self.run()
        return


if __name__ == '__main__':
    import doctest

    doctest.testmod()
