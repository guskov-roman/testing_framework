import abc
import collections
import contextlib
import copy
import itertools
import re
import select
import sys
import termios
import time
import tty
import typing

ConIO = typing.TypeVar("ConIO", bound="ConnectorIO")

class ConnectorException(Exception):
    """Connector Exception """
    pass


class ConnectorIO(typing.ContextManager):
    # generic channel interface {{{
    @abc.abstractmethod
    def write(self, buf: bytes) -> int:
        pass

    @abc.abstractmethod
    def read(self, n: int, timeout: typing.Optional[float] = None) -> bytes:
        pass

    @abc.abstractmethod
    def close(self) -> None:
        pass

    @abc.abstractmethod
    def fileno(self) -> int:
        pass

    @property
    @abc.abstractmethod
    def closed(self) -> bool:
        pass

    @abc.abstractmethod
    def update_pty(self, columns: int, lines: int) -> None:
        pass

    def __enter__(self: ConIO) -> ConIO:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

class ChannelBorrowed(ConnectorIO):  # pragma: no cover
    exception: typing.Type[Exception] = ConnectorException 

    def write(self, buf: bytes) -> int:
        raise self.exception()

    def read(self, n: int, timeout: typing.Optional[float] = None) -> bytes:
        raise self.exception()

    def close(self) -> None:
        raise self.exception()

    def fileno(self) -> int:
        raise self.exception()

    @property
    def closed(self) -> bool:
        raise self.exception()

    def update_pty(self, columns: int, lines: int) -> None:
        raise self.exception()

class ChannelTaken(ChannelBorrowed):  # pragma: no cover
    exception: typing.Type[Exception] = ConnectorException 

    def close(self) -> None:
        pass

    @property
    def closed(self) -> bool:
        return True
    

class BoundedPattern:
    _length: int

    def __init__(self, pattern: typing.Pattern[bytes]) -> None:
        self.pattern = pattern

        import sre_parse

        parsed = sre_parse.parse(
            typing.cast(str, self.pattern.pattern), flags=self.pattern.flags
        )
        width = parsed.getwidth()
        if isinstance(width, int):
            self._length = width
        elif isinstance(width, tuple):
            self._length = width[1]

        # It seems that when MAXREPEAT is not defined, the values 2**32-1 or
        # 2**64-1 are used.  Let's be portable by just interpreting any value
        # above 2**16-1 as an unbounded length.  Surely nobody wants to
        # intentionally match 64 kiB patterns.......
        if self._length >= getattr(sre_parse, "MAXREPEAT", 2**16 - 1):
            raise tbot.error.UnboundedPatternError(self.pattern.pattern)

    def __len__(self) -> int:
        return self._length

SearchString = typing.Union[bytes, BoundedPattern]
ConvenientSearchString = typing.Union[SearchString, typing.Pattern, str]

def _convert_search_string(string: ConvenientSearchString) -> SearchString:
    if isinstance(string, str):
        return string.encode()
    elif isinstance(string, typing.Pattern):
        return BoundedPattern(string)
    else:
        return string

class ExpectResult(typing.NamedTuple):
    i: int
    match: typing.Union[str, typing.Match[bytes]]
    before: str
    after: str

class Connector(typing.ContextManager):

    def __init__(self, connector_io: ConnectorIO) -> None:
        self._c = connector_io
        self.prompt: typing.Optional[bytes] = None
        # self.death_strings: typing.List[
        #     typing.Tuple[
        #         SearchString, typing.Type[DeathStringException], typing.Deque[int]
        #     ]
        # ] = []
        # self._streams: typing.List[typing.TextIO] = []
        # self._streambuf = bytearray()
        self._log_prompt = True
        self._write_blacklist: typing.List[int] = []

        self.slow_send_delay: typing.Optional[float] = None
        self.slow_send_chunksize: int = 32

    # def load_config(self, name = "GenericConnector", config):

    def write(self, buf: bytes, _ignore_blacklist: bool = False) -> None:
        if not _ignore_blacklist:
            for blacklisted in self._write_blacklist:
                if blacklisted in buf:
                    raise ConnectorException(
                        f"attempted to write a forbidden byte ({chr(blacklisted)!r})"
                    )

        cursor = 0
        while cursor < len(buf):
            if self.slow_send_delay is None:
                # write as much as possible
                bytes_written = self._c.write(buf[cursor:])
            else:
                # write at most `slow_send_chunksize` bytes
                bytes_written = self._c.write(buf[cursor:][: self.slow_send_chunksize])
                # and then wait for `slow_send_delay` before sending the next chunk
                time.sleep(self.slow_send_delay)
            cursor += bytes_written

    # Size of individual read calls.
    READ_CHUNK_SIZE = 4096

    def read(self, n: int = -1, timeout: typing.Optional[float] = None) -> bytes:
        if n < 0:
            # Block first and then read non-blocking
            buf = bytearray(self._c.read(self.READ_CHUNK_SIZE, timeout))
            reader = self.read_iter(timeout=0.0)
        else:
            # Read n bytes non-blocking
            buf = bytearray()
            reader = self.read_iter(max=n, timeout=timeout)

        try:
            for chunk in reader:
                buf += chunk
        except TimeoutError:
            if n != -1:
                # print("timeout Error")
                # return bytes()
                raise

        assert (n == -1) or (len(buf) == n)
        return buf

    def read_iter(
        self, max: int = sys.maxsize, timeout: typing.Optional[float] = None
    ) -> typing.Iterator[bytes]:
        start_time = time.monotonic()

        bytes_read = 0
        while True:
            timeout_remaining = None
            if timeout is not None:
                timeout_remaining = timeout - (time.monotonic() - start_time)
                if timeout_remaining <= 0:
                    # print("Timeout Error")
                    raise TimeoutError()

            max_read = min(self.READ_CHUNK_SIZE, max - bytes_read)
            new = self._c.read(max_read, timeout_remaining)
            bytes_read += len(new)
            # self._write_stream(new)
            # self._check(new)
            yield new

            assert bytes_read <= max, "read overflow"
            if bytes_read == max:
                break

    def read_until_prompt(
        self,
        prompt: typing.Optional[ConvenientSearchString] = None,
        timeout: typing.Optional[float] = None,
    ) -> str:

        buf = bytearray()

        for new in self.read_iter(timeout=timeout):
            buf += new

            if isinstance(self.prompt, bytes):
                if buf.endswith(self.prompt):
                    return (
                        buf[: -len(self.prompt)]
                        .decode("utf-8", errors="replace")
                        .replace("\r\n", "\n")
                        .replace("\n\r", "\n")
                    )
            elif isinstance(self.prompt, BoundedPattern):
                match = self.prompt.pattern.search(buf)
                if match is not None:
                    return (
                        buf[: match.span()[0]]
                        .decode("utf-8", errors="replace")
                        .replace("\r\n", "\n")
                        .replace("\n\r", "\n")
                    )

        raise RuntimeError("unreachable")  # pragma: no cover           

    # file-like interface {{{
    def fileno(self) -> int:
        return self._c.fileno()

    def close(self) -> None:
        self._c.close()

    @property
    def closed(self) -> bool:
         return self._c.closed

    # context manager {{{
    def __enter__(self) -> "Channel":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:  # type: ignore
        if not self.closed:
            self.close()

    # pexpect-like interface {{{
    def send(
        self,
        s: typing.Union[str, bytes],
        read_back: bool = False,
        timeout: typing.Optional[float] = None,
        _ignore_blacklist: bool = False,
    ) -> None:
        # Do nothing for empty strings
        if s == "" or s == b"":
            return

        s = s.encode("utf-8") if isinstance(s, str) else s

        # Let's not overwhelm the channel-io by sending too much at once...
        s_iter = iter(s)
        while True:
            chunk = bytes(itertools.islice(s_iter, 512))
            if chunk == b"":
                break

            self.write(chunk, _ignore_blacklist=_ignore_blacklist)

            if read_back:
                # Read back what was just sent.  Assume a well-behaved other side
                # and read two characters for every '\r' or '\n' sent.  This might
                # be flawed in some cases, though ...
                length = len(chunk) + chunk.count(b"\r") + chunk.count(b"\n")
                self.read(n=length, timeout=timeout)

    def sendline(
        self,
        s: typing.Union[str, bytes] = "",
        read_back: bool = False,
        timeout: typing.Optional[float] = None,
    ) -> None:
        s = s.encode("utf-8") if isinstance(s, str) else s
        # The "Enter" key sends '\r'
        self.send(s + b"\r", read_back, timeout)

    def sendcontrol(self, c: str) -> None:
        assert len(c) == 1, "Only a single character is allowed for sendcontrol()"

        num = ord(c) - 64
        print(f"send num {num}")
        assert 0 <= num <= 0x1F, f"Character {c!r} does not represent a control char!"

        self.write(bytes([num]), _ignore_blacklist=True)

    def sendeof(self) -> None:
        raise NotImplementedError()

    def sendintr(self) -> None:
        """Send ``CTRL-C`` to this channel."""
        self.sendcontrol("C")

    def readline(
        self,
        timeout: typing.Optional[float] = None,
        lineending: typing.Union[str, bytes] = "\r\n",
    ) -> str:

        if isinstance(lineending, str):
            end = lineending.encode("utf-8")
        else:
            end = lineending

        # This implementation is quite naive but any
        # other way would possibly read too much :/

        start_time = time.monotonic()
        line = bytearray()

        while True:
            timeout_remaining = None
            if timeout is not None:
                timeout_remaining = timeout - (time.monotonic() - start_time)

            c = self.read(1, timeout=timeout_remaining)
            line.extend(c)

            if line.endswith(end):
                break

        # print(f"read line: {line} ")

        return (
            line.decode("utf-8", errors="replace")
            .replace("\r\n", "\n")
            .replace("\n\r", "\n")
        )

    def expect(
        self,
        patterns: typing.Union[
            ConvenientSearchString, typing.List[ConvenientSearchString]
        ],
        timeout: typing.Optional[float] = None,
    ) -> ExpectResult:

        if not isinstance(patterns, list):
            pattern_list = [_convert_search_string(patterns)]
        else:
            pattern_list = [_convert_search_string(pat) for pat in patterns]

        buf = bytearray()
        for chunk in self.read_iter(timeout=timeout):
            buf.extend(chunk)

            for pattern_index, pat in enumerate(pattern_list):
                if isinstance(pat, bytes):
                    index = buf.find(pat)
                    if index != -1:
                        return ExpectResult(
                            pattern_index,
                            pat.decode("utf-8", errors="replace"),
                            buf[:index]
                            .decode("utf-8", errors="replace")
                            .replace("\r\n", "\n")
                            .replace("\n\r", "\n"),
                            buf[index + len(pat) :]
                            .decode("utf-8", errors="replace")
                            .replace("\r\n", "\n")
                            .replace("\n\r", "\n"),
                        )
                elif isinstance(pat, BoundedPattern):
                    match = pat.pattern.search(buf)
                    if match is not None:
                        return ExpectResult(
                            pattern_index,
                            match,
                            buf[: match.span(0)[0]]
                            .decode("utf-8", errors="replace")
                            .replace("\r\n", "\n")
                            .replace("\n\r", "\n"),
                            buf[match.span(0)[1] :]
                            .decode("utf-8", errors="replace")
                            .replace("\r\n", "\n")
                            .replace("\n\r", "\n"),
                        )
                else:
                    raise AssertionError(
                        f"expect pattern has unknown type: {pat.__class__!r}"
                    )

        raise Exception("reached end of stream without pattern appearing")

    def take(self) -> "Connector":
        con_io = self._c
        self._c = ChannelTaken()
        new = copy.deepcopy(self)
        new._c = con_io
        return new

    def attach_interactive(
        self, end_magic: typing.Union[str, bytes, None] = None, ctrld_exit: bool = False
    ) -> None:
        end_magic_bytes = (
            end_magic.encode("utf-8") if isinstance(end_magic, str) else end_magic
        )

        end_ring_buffer: typing.Deque[int] = collections.deque(
            maxlen=len(end_magic_bytes) if end_magic_bytes is not None else 1
        )

        # During an interactive session, the blacklist should not apply
        old_blacklist = self._write_blacklist
        self._write_blacklist = []

        escape_timestamp = None
        previous: typing.Deque[int] = collections.deque(maxlen=3)

        if not ctrld_exit:
            print("Press CTRL+] three times within 1 second to exit.")
            # tbot.log.message(
            #     tbot.log.c("Press CTRL+] three times within 1 second to exit.").bold
            # )

        oldtty = termios.tcgetattr(sys.stdin)
        try:
            tty.setraw(sys.stdin.fileno())
            tty.setcbreak(sys.stdin.fileno())

            mode = termios.tcgetattr(sys.stdin)
            special_chars = mode[6]
            assert isinstance(special_chars, list)
            special_chars[termios.VMIN] = b"\0"
            special_chars[termios.VTIME] = b"\0"
            termios.tcsetattr(sys.stdin, termios.TCSAFLUSH, mode)

            while True:
                r, _, _ = select.select([self, sys.stdin], [], [])

                if self in r:
                    data = self._c.read(4096)
                    if isinstance(end_magic_bytes, bytes):
                        end_ring_buffer.extend(data)
                        for a, b in itertools.zip_longest(
                            end_ring_buffer, end_magic_bytes
                        ):
                            if a != b:
                                # print("break 1")
                                break
                        else:
                            # print("break 2")
                            break
                    sys.stdout.buffer.write(data)
                    sys.stdout.buffer.flush()
                if sys.stdin in r:
                    data = sys.stdin.buffer.read(4096)
                    previous.extend(data)

                    # Old ^D to exit behavior
                    if ctrld_exit and data == b"\x04":
                        break

                    # Detect whether ^] was pressed 3 times in 1 second
                    now = time.monotonic()
                    if escape_timestamp is None and 0x1D in previous:
                        escape_timestamp = now
                    elif escape_timestamp is not None:
                        if now < escape_timestamp + 1:
                            if previous.count(0x1D) >= 3:
                                print("break 3")
                                break
                        else:
                            escape_timestamp = None

                    self.send(data)

            sys.stdout.write("\r\n")
        finally:
            self._write_blacklist = old_blacklist
            termios.tcsetattr(sys.stdin, termios.TCSADRAIN, oldtty)

    # }}}

    # pexpect-like }}}

# res = ExpectResult(0, "first", "string", "after")

# print(type(res))
