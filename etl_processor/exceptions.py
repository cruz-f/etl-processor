"""It contains custom exceptions that are raised during the ETL process."""


class ETLException(Exception):
    """
    A ETLException is an exception that is raised during the ETL process.

    Parameters
    ----------
    code : str
        A string that represents the error code (e.g. 'ETL-001').
    description : str
        A string that describes the error.
    """

    _code = 'ETL-000'

    def __init__(self, description: str):
        self._description = description

    @property
    def code(self) -> str:
        """
        A string that represents the error code (e.g. 'ETL-001').

        Returns
        -------
        str
            The error code.
        """
        return self._code

    @property
    def description(self) -> str:
        """
        A string that describes the error. It should be a human-readable message (e.g. 'The ETL process failed').

        Returns
        -------
        str
            The error description.
        """
        return self._description

    def __str__(self) -> str:
        return f'{self.code}: {self.description}'

    def __repr__(self) -> str:
        return f'ETLException(code={self.code}, description={self.description})'


class ValidationError(Exception):
    """A ValidationError is an exception that is raised when a validation error occurs during the ETL process."""

    _code = 'ETL-001'


class NetworkError(Exception):
    """A NetworkError is an exception that is raised when a network error occurs during the ETL process."""

    _code = 'ETL-002'


class ExtractionError(Exception):
    """An ExtractionError is an exception that is raised when an extraction error occurs during the ETL process."""

    _code = 'ETL-003'


class TransformationError(Exception):
    """A TransformationError is an exception that is raised when a transformation error occurs during the ETL process."""

    _code = 'ETL-004'


class LoadError(Exception):
    """A LoadError is an exception that is raised when a load error occurs during the ETL process."""

    _code = 'ETL-005'
