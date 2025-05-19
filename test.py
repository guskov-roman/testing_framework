import connectors
import typing
import io

from connectors.shell_connector import ShellConnector

consoleBuffer = None

with ShellConnector() as sh:
    # sh.log = "output.log"
    sh.sendline("ls /etc", read_back=True)
    try:
        out = sh.expect0("dhcpf", 1)
        print(f"out: {out}")
    except TimeoutError as ex:
        print(f"not found {ex}")
    # out = sh.read_until_prompt()
    consoleBuffer = sh.connectorBuffer

print("******* Console Buffer *******")
print(consoleBuffer.decode())

# with ShellConnector().open_interactive("mc") as mc:
#     out = None
#     while True:
#         try:
#             out = mc.readline(0.3)
#             print(out, end="")
#         except:
#             break
