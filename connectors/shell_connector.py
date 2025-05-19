import fcntl
import os
import pty
import select
import struct
import subprocess
import termios
import time
import typing
import re
import shlex
import shutil
import typing
import abc

from connectors import errors
from connectors import connector

ConnectorException = errors.ConnectorError
ConnectorClosedException = errors.ConnectorClosedError

class ShellConnectorIO(connector.ConnectorIO):

    _MIN_READ_WAIT = 0.3

    def __init__(self) -> None:
        self.pty_master, self.pty_slave = pty.openpty()
        self.log = None

        self.p = subprocess.Popen(
            ["bash", "--norc", "--noprofile", "--noediting", "-i"],
            stdin=self.pty_slave,
            stdout=self.pty_slave,
            stderr=self.pty_slave,
            start_new_session=True,
        )

        flags = fcntl.fcntl(self.pty_master, fcntl.F_GETFL)
        flags = flags | os.O_NONBLOCK
        fcntl.fcntl(self.pty_master, fcntl.F_SETFL, flags)

    def write(self, buf: bytes) -> int:
        if self.closed:
            raise ConnectorException
        
        _, w, _ = select.select([], [self.pty_master], [], 10.0)
        if self.pty_master not in w:
            raise TimeoutError("write timeout exceeded")

        bytes_written = os.write(self.pty_master, buf)
        if bytes_written == 0:
            raise ConnectorException 
        return bytes_written

    def read(self, n: int, timeout: typing.Optional[float] = None) -> bytes:
        if not self.closed:
   
            end_time = None if timeout is None else time.monotonic() + timeout
            while True:
                if end_time is None:
                    select_timeout = self._MIN_READ_WAIT
                else:
                    select_timeout = min(self._MIN_READ_WAIT, end_time - time.monotonic())
                    if select_timeout <= 0:
                        raise TimeoutError()

                r, _, _ = select.select([self.pty_master], [], [], select_timeout)

                if self.pty_master in r:
                    # There is something to read, proceed to reading it.
                    break
                elif self.closed:
                    # Nothing to read and channel is closed.  We're done for good.
                    raise ConnectorException
        try:
            return os.read(self.pty_master, n)
        except (BlockingIOError, OSError):
            raise ConnectorException

    def close(self) -> None:
        if self.closed:
            raise ConnectorClosedException

        sid = os.getsid(self.p.pid)
        self.p.terminate()
        os.close(self.pty_slave)
        os.close(self.pty_master)
        try:
            self.p.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            self.p.kill()
            self.p.communicate()

        wait_total = 0.0
        for t in range(10):
            if (
                subprocess.call(
                    ["ps", "-s", str(sid)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                != 0
            ):
                break

            wait_time = 2**t / 100

            if t >= 7:
                offending = (
                    subprocess.run(
                        ["ps", "-s", str(sid), "ho", "args"],
                        stdout=subprocess.PIPE,
                        encoding="utf-8",
                    )
                    .stdout.strip()
                    .split("\n")
                )
                offending_str = "\n".join(f" - {args!r}" for args in offending)

            time.sleep(wait_time)
            wait_total += wait_time
        else:
            raise ConnectorException("some subprocess(es) did not stop")

    def fileno(self) -> int:
        return self.pty_master

    @property
    def closed(self) -> bool:
        self.p.poll()
        return self.p.returncode is not None

    def update_pty(self, columns: int, lines: int) -> None:
        s = struct.pack("HHHH", lines, columns, 0, 0)
        fcntl.ioctl(self.pty_master, termios.TIOCSWINSZ, s, False)

class ShellConnector(connector.Connector):

    def __init__(self) -> None:
        super().__init__(ShellConnectorIO())
        self.name = "ShellConnector"
        self.prompt = b"BTR-VEJPVC1QUk9NUFQK$ "

        self._write_blacklist = [
                0x03,  # ETX  | End of Text / Interrupt
                0x04,  # EOT  | End of Transmission
                0x11,  # DC1  | Device Control One (XON)
                0x12,  # DC2  | Device Control Two
                0x13,  # DC3  | Device Control Three (XOFF)
                0x14,  # DC4  | Device Control Four
                0x15,  # NAK  | Negative Acknowledge
                0x16,  # SYN  | Synchronous Idle
                0x17,  # ETB  | End of Transmission Block
                0x1A,  # SUB  | Substitute / Suspend Process
                0x1C,  # FS   | File Separator
                0x7F,  # DEL  | Delete               
        ]

        self.sendline(
            b"PROMPT_COMMAND=''; PS1='"
            + self.prompt[:6]
            + b"''"
            + self.prompt[6:]
            + b"'",
        )
        self.read_until_prompt()

        self.sendline("unset HISTFILE")
        self.read_until_prompt()

        # Disable line editing
        self.sendline("set +o emacs; set +o vi")
        self.read_until_prompt()

        # Set secondary prompt to ""
        self.sendline("PS2=''")
        self.read_until_prompt()

        self.sendline("histchars=''")
        self.read_until_prompt()       

        # Set terminal size
        termsize = shutil.get_terminal_size()
        self.sendline(f"stty cols {max(80, termsize.columns - 48)}")
        self.read_until_prompt()
        self.sendline(f"stty rows {termsize.lines}")
        self.read_until_prompt()

    def _posix_fetch_return_code(self) -> int:
        self.sendline("echo $?", read_back=True)
        retcode_str = self.read_until_prompt()
        try:
            return int(retcode_str)
        except ValueError:
            raise tbot.error.InvalidRetcodeError(mach, retcode_str) from None

    def exec(self, cmd) -> typing.Tuple[int, str]:
        self.sendline(cmd, read_back=True)
        out = self.read_until_prompt()
        retcode = self._posix_fetch_return_code()
        return (retcode, out)

    def open_interactive(self, cmd):
        self.sendline("stty -isig", read_back=True)
        self.read_until_prompt()
        self.sendline(cmd + "; exit", read_back=True)
        return self.take()

    def escape(
        self, *args: typing.Union[str]
    ) -> str:
        string_args = []
        for arg in args:
            if isinstance(arg, str):
                string_args.append(shlex.quote(arg))
            elif isinstance(arg, linux_shell.Special):
                string_args.append(arg._to_string(self))
            elif isinstance(arg, path.Path):
                string_args.append(shlex.quote(arg.at_host(self)))
            else:
                raise TypeError(f"{type(arg)!r} is not a supported argument type!")

        return " ".join(string_args)   
