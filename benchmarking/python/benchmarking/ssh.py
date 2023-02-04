import socket
from typing import Tuple

import paramiko
import scp

def ssh_command(host, command, timeout=None, verbose=False, user="hendrik.makait") -> Tuple[int, str]:
    try:
        with paramiko.SSHClient() as client:
            stdout, stderr = _ssh_command(client, host, command, timeout=timeout, user=user)
            # Wait for command to finish
            output = str(stdout.read(), "utf-8")
            status = stdout.channel.recv_exit_status()
            if verbose:
                print(f"Channel return code for command {command} is {status}")
            return status, output
    except paramiko.SSHException as e:
        print(f"SSHException {e}")
        raise
    except socket.timeout:
        print("SSH Pipe timed out...")


def _ssh_command(client, host, command, timeout, user):
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    retries = 0
    max_num_retries = 3
    while retries < max_num_retries:
        try:
            client.connect(host, username=user)
            break
        except (paramiko.SSHException, OSError) as e:
            retries += 1
            if retries == max_num_retries:
                raise e

    _, stdout, stderr = client.exec_command(command, timeout=timeout)
    return stdout, stderr


def download(path, host, user="hendrik.makait") -> None:
    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host, username=user)
        with scp.SCPClient(ssh.get_transport()) as client:
            return client.get(path)

