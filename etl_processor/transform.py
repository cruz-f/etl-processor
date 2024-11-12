"""Implementation of the FIRDS transformation tool."""

from pathlib import Path

import pandas as pd

from etl_processor.exceptions import TransformationError
from etl_processor.tool import Tool


class FIRDSTransformer(Tool):
    """
    Transformation tool to obtain new insights from the financial instruments in the financial instrument reference data system (FIRDS).

    Attributes
    ----------
    data_dir : str | Path
        The directory to read and save the extracted FIRDS documents.
    chunk_size : int
        The size of the chunks to process the FIRDS data.

    Examples
    --------
    >>> firds_transformer = FIRDSTransformer(
    ...     data_dir='data',
    ... )
    >>> firds_transformer.run()
    """

    def __init__(
        self,
        data_dir: str | Path,
        chunk_size: int = 10**6,
    ) -> None:
        """
        Initialize the FIRDS transformation tool.

        Parameters
        ----------
        data_dir : str | Path
            The directory to read and save the extracted FIRDS documents.
        chunk_size : int, optional
            The size of the chunks to process the FIRDS data, by default 10**6.
        """
        self.chunk_size = chunk_size
        self.data_dir = Path(data_dir)

        self.firds_csv_path = self.data_dir / 'firds.csv'

    async def arun(self) -> None:
        """
        Transform the FIRDS data. Asynchronous version.
        It calculates the total number of the letter "a" in the full name of the financial instruments.
        It adds a new column indicating whether the financial instrument full name contains the letter "a".

        Raises
        ------
        TransformationError
            If an error occurs during the transformation of the FIRDS data.
        """
        self.run()
        return

    def run(self) -> None:
        """
        Transform the FIRDS data.
        It calculates the total number of the letter "a" in the full name of the financial instruments.
        It adds a new column indicating whether the financial instrument full name contains the letter "a".

        Raises
        ------
        TransformationError
            If an error occurs during the transformation of the FIRDS data.
        """
        if not self.data_dir.exists():
            raise TransformationError(f'The data directory {self.data_dir} does not exist.')

        if not self.firds_csv_path.exists():
            raise TransformationError(f'The FIRDS CSV file {self.firds_csv_path} does not exist.')

        try:
            # process the firds csv file in chunks
            transformed_csv_path = self.data_dir / 'firds_transformed.csv'
            first_chunk = True

            with pd.read_csv(self.firds_csv_path, chunksize=self.chunk_size) as reader:
                for chunk in reader:
                    # calculate the total number of the letter "a" in the full name
                    chunk['a_count'] = chunk['FinInstrmGnlAttrbts.FullNm'].str.count('a')

                    # add a new column indicating whether the financial instrument full name contains the letter "a"
                    chunk['contains_a'] = chunk['FinInstrmGnlAttrbts.FullNm'].str.contains('a')

                    # save the transformed data
                    if first_chunk:
                        chunk.to_csv(transformed_csv_path, mode='w', header=True, index=False)
                    else:
                        chunk.to_csv(transformed_csv_path, mode='a', header=False, index=False)

                    first_chunk = False

        except Exception as exc:
            raise TransformationError('Error transforming the FIRDS data.') from exc

        return


if __name__ == '__main__':
    import doctest

    doctest.testmod()
