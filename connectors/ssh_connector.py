import getpass
import paramiko
import pathlib
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
    
    @property
    def hostname(self):
        return self._hostname

    @hostname.setter
    def hostname(self, hostname):
        self._hostname = hostname

    @property
    def port(self):
        return self._port

    @hostname.setter
    def port(self, port):
        self._port = port

    @property
    def username(self):
        return self._username

    @username.setter
    def username(self, username):
        self._username = username

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, password):
        self._password = password

    @property
    def key_file(self):
        return self._file

    @key_file.setter
    def key_file(self, key_file):
        self._key_file = key_file

    @property
    def ignore_hostkey(self):
        return self._ignore_hostkey

    @ignore_hostkey.setter
    def ignore_hostkey(self, ignore_hostkey):
        self.ignore_hostkey = ignore_hostkey

    def __init__(self,  **config) -> None:
        self._hostname = config.get('hostname')
        self._port = config.get('hostname')
        self._username = config.get('hostname')
        self._password = config.get('password')
        self._ignore_hostkey = config.get('ignore_hostkey')
        self._key_file = config.get("key_file")
        self._client = None
        # self._auto_connect = config.get("auto_connect", False)

    def open(self) -> None:
        if self._client is None:
            self._client = paramiko.SSHClient()

            try:
                c = paramiko.config.SSHConfig()
                with open(pathlib.Path.home() / ".ssh" / "config") as cfg:
                    c.parse(cfg)
                self._config = c.lookup(self.hostname)
                print(f"config: {self._config}")
            except FileNotFoundError:
                # Config file does not exist
                pass
            except Exception as e:
                print("Invalid" + f" .ssh/config: {str(e):s}")
                # Invalid config
                # print(("Invalid").red + f" .ssh/config: {str(e):s}")
                # pass

            if self.ignore_hostkey:
                self._client.set_missing_host_key_policy(
                    paramiko.client.AutoAddPolicy()
                )
            else:
                self._client.load_system_host_keys()

            self._client.connect(
                    self._hostname,
                    username=self._username,
                    port=self._port,
                    password=self._password,
                    key_filename=self._key_file
            )

            super().__init__(SSHConnectorIO(self._client.get_transport().open_session()))

            PROMPT = b"MY-VEJPVC1QUk9NUFQK$ "

            self.send_line(
                b"PROMPT_COMMAND=''; PS1='"
                + PROMPT[:6]
                + b"''"
                + PROMPT[6:]
                + b"'",
            )
            self.prompt = PROMPT
            # print("read promt")
            # self.read_until_prompt()
            # print("end read promt")
