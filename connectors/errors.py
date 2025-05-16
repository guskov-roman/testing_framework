class ConnectorError(Exception):
    """
    Base-class for exceptions raised due to connector errors
    """

class ConnectorClosedError(ConnectionError):
     """
    Error type for exceptions when a connector was closed unexpectedly.

    This error is raised when interaction with a connector is attemped which was
    either closed explicitly beforehand or which was closed unexpectedly by the
    remote end.

    """

class InvalidRetcodeError(ConnectorError):
    """
    While trying to fetch the return code of a command, unexpected output was received.
    """

    def __init__(
        self,
        retcode_str: str,
    ) -> None:
        self.retcode_str = retcode_str
        super().__init__(
            f"received string {retcode_str!r} instead of a return code integer"
        )
