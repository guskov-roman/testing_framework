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

# import tbot.error

import Connector 

READ_CHUNK_SIZE = 4096
MIN_READ_WAIT = 0.3


class ShellConnectorIO(Connector.ConnectorIO):

    def __init__(self) -> None:
        self.pty_master, self.pty_slave = pty.openpty()

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
            raise BaseConnector.ConnectorException
        
        _, w, _ = select.select([], [self.pty_master], [], 10.0)
        if self.pty_master not in w:
            raise TimeoutError("write timeout exceeded")

        bytes_written = os.write(self.pty_master, buf)
        if bytes_written == 0:
            raise Connector.ConnectorException
        return bytes_written

    def read(self, n: int, timeout: typing.Optional[float] = None) -> bytes:
        if not self.closed:
   
            end_time = None if timeout is None else time.monotonic() + timeout
            while True:
                if end_time is None:
                    select_timeout = MIN_READ_WAIT
                else:
                    select_timeout = min(MIN_READ_WAIT, end_time - time.monotonic())
                    if select_timeout <= 0:
                        raise TimeoutError()

                r, _, _ = select.select([self.pty_master], [], [], select_timeout)

                if self.pty_master in r:
                    # There is something to read, proceed to reading it.
                    break
                elif self.closed:
                    # Nothing to read and channel is closed.  We're done for good.
                    raise BaseConnector.ConnectorException

                # Loop back around and try again until timeout expires.

        try:
            # return channel._debug_log(self, os.read(self.pty_master, n))
            return os.read(self.pty_master, n)
        except (BlockingIOError, OSError):
            raise BaseConnector.ConnectorException

    def close(self) -> None:
        if self.closed:
            raise tbot.error.ChannelClosedError

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
            raise BaseConnector.ConnectorException("some subprocess(es) did not stop")

    def fileno(self) -> int:
        return self.pty_master

    @property
    def closed(self) -> bool:
        self.p.poll()
        return self.p.returncode is not None

    def update_pty(self, columns: int, lines: int) -> None:
        s = struct.pack("HHHH", lines, columns, 0, 0)
        fcntl.ioctl(self.pty_master, termios.TIOCSWINSZ, s, False)

BTR_PROMPT = b"BTR-VEJPVC1QUk9NUFQK$ "

class ShellConnector(Connector.Connector):
    def __init__(self) -> None:
        super().__init__(ShellConnectorIO())
        self.name = "PtyConnector"
        self.sendline(
            b"PROMPT_COMMAND=''; PS1='"
            + BTR_PROMPT[:6]
            + b"''"
            + BTR_PROMPT[6:]
            + b"'",
        )
        self.prompt = BTR_PROMPT
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

    def open_channel(self, cmd):
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

    def interactive(self) -> None:
        # Generate the endstring instead of having it as a constant
        # so opening this files won't trigger an exit
        endstr = (
            "INTERACTIVE-END-"
            + hex(165_380_656_580_165_943_945_649_390_069_628_824_191)[2:]
        )

        termsize = shutil.get_terminal_size()
        self.sendline(self.escape("stty", "cols", str(termsize.columns)))
        self.sendline(self.escape("stty", "rows", str(termsize.lines)))

        # Outer shell which is used to detect the end of the interactive session
        self.sendline(f"bash --norc --noprofile")
        self.sendline(f"PS1={endstr}")
        self.read_until_prompt(prompt=endstr)

        # Inner shell which will be used by the user
        self.sendline("bash --norc --noprofile")
        self.sendline("set -o emacs")
        prompt = self.escape(
            f"\\[\\033[36m\\]{self.name}: \\[\\033[32m\\]\\w\\[\\033[0m\\]> "
        )
        self.sendline(f"PS1={prompt}")

        self.read_until_prompt(prompt=re.compile(b"> (\x1B\\[.{0,10})?"))
        self.sendline()
        # tbot.log.message("Entering interactive shell ...")
        print("Entering interactive shell ...")

        self.attach_interactive(end_magic=endstr)

        print("Exiting interactive shell ...")

        try:
            self.sendline("exit")
            try:
                self.read_until_prompt(timeout=0.5)
            except TimeoutError:
                # we might still be in the inner shell so let's try exiting again
                self.sendline("exit")
                self.read_until_prompt(timeout=0.5)
        except TimeoutError:
            raise Exception("Failed to reacquire shell after interactive session!")   



# with ShellConnector() as c:
#     c.sendline("ls /etc", read_back=True)
#     out = c.read_until_prompt()
#     print(out)

    # out = c.read_until_prompt()
    # print(out)
# print(f"ord: {ord('A')}")
# print("exit")
con = ShellConnector()

# # print("start mc")
with con.open_channel("mc") as mc:
    # mc.write(bytes([121]), _ignore_blacklist=True)
    output = None
    while True:
        try:
            out = mc.readline(0.3) 
            print(out, end="")
            # output = output + out
        except:
            break

#     print(output)
    # out = mc.read(256)
    # print(out)
    # print("*****************************************************************************************************")
    # mc.sendline("ls /etc")
    # out = mc.expect("Hint: Want your plain")
    # out = mc.expect("fdsfsdfsdf", 0.3)
    # print(out)

#     mc.sendcontrol("C")    
#     print("send succsess")
#     out = mc.read(256)
#     print(f"read: {out}")
#     # out = mc.read_until_prompt()
#     # print("out: {out}")
# print("wait for end")
# out = con.exec("mc")
# print(f"out: {out}")

# con.close()

