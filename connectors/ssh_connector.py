import paramiko
import socket
import typing

from connectors import errors
from connectors import connector

ConnectorException = errors.ConnectorError
ConnectorClosedException = errors.ConnectorClosedError

class SSHConnectorIO(connector.ConnectorIO):

    def __init__(self, channel: paramiko.Channel) -> None:
        self.channel = channel
        self.channel.get_pty("xterm-256color", 80, 25, 1024, 1024)
        self.channel.invoke_shell()
        self.channel.settimeout(0.0)

    def write(self, buf: bytes) -> int:
        if self.closed:
            raise ConnectorClosedException()

        # channel._debug_log(self, buf, True)
        bytes_written = self.channel.send(buf)
        if bytes_written == 0:
            raise ConnectorClosedException()
        return bytes_written

    def read(self, n: int, timeout: typing.Optional[float] = None) -> bytes:
        self.channel.settimeout(timeout)

        try:
            return self.channel.recv(n)
        except socket.timeout:
            raise TimeoutError()
        finally:
            self.channel.settimeout(0.0)

    def close(self) -> None:
        if self.closed:
            raise ConnectorClosedException()

        self.channel.close()

    def fileno(self) -> int:
        return self.channel.fileno()

    @property
    def closed(self) -> bool:
        return self.channel.exit_status_ready()

    def update_pty(self, columns: int, lines: int) -> None:
        self.channel.resize_pty(columns, lines, 1024, 1024)


class SSHConnector(connector.Connector):
    def __init__(self,  **config) -> None:
        self.hostname = config.get('hostname')
        self.username = config.get('hostname')
        self.password = config.get('password')
        self.key = config.get("ssh_key")
        self.auto_connect = config.get("auto_connect", False)

    def open():
        super().__init__()

        
