import connectors

from connectors.shell_connector import ShellConnector

with ShellConnector() as sh:
    sh.sendline("ls /etc", read_back=True)
    out = sh.read_until_prompt()
    print(out)

with ShellConnector().open_interactive("minicom -D /dev/ttyUSB0") as mc:
    out = None
    while True:
        try:
            out = mc.readline(0.3)
            print(out, end="")
        except:
            break
