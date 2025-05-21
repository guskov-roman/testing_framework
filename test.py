import connectors
import typing
import io

from connectors.shell_connector import ShellConnector
from connectors.ssh_connector import SSHConnector

# with open("/home/baikal.int/r.guskov/.ssh/id_rsa.pub", 'r') as f:
#     print(f.read())


with SSHConnector() as ssh:
    ssh.hostname = "192.168.52.126"
    ssh.port = 22
    ssh.username = "roman"
    # ssh.password = "ra4csn"
    ssh.key_file = "/home/baikal.int/r.guskov/.ssh/id_rsa.pub"
    ssh.open()
    ssh.send_line("ls /home", read_back=True)
    # out = ssh.read_until_prompt()
    # print(out)
    
    while True:
        try:
            out = ssh.read_line(0.2)
            print(out, end="")
        except:
            break
    # print("close()")
    # out = ssh.read_line()
    # print(out)
    # out = ssh.read_until_prompt()
    # print(out)

# consoleBuffer = None

# with ShellConnector() as sh:
#     out = sh.exec("ls /etc")
#     # print(out[0])
#     file.close()
    # print("send command")
    # sh.send_line("ls /etc", read_back=True)
    # ret = sh.expect(["magic", "glvndf"], 1)
    # print(ret)
    # out = sh.read(256, 0.1)
    # print(out.decode())
    # # sh.log = "output.log"
    # sh.sendline("ls /etc", read_back=True)
    # try:
    #     out = sh.expect0("dhcpf", 1)
    #     print(f"out: {out}")
    # except TimeoutError as ex:
    #     print(f"not found {ex}")
    # # out = sh.read_until_prompt()
    # consoleBuffer = sh.connectorBuffer

# print("******* Console Buffer *******")
# print(consoleBuffer.decode())

# with ShellConnector().open_interactive("mc") as mc:
#     out = None
#     while True:
#         try:
#             out = mc.readline(0.3)
#             print(out, end="")
#         except:
#             break
