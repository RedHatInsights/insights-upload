from subprocess import Popen, PIPE, STDOUT

import requests


def run(cmd, fatal=None, cwd=None, stdout=PIPE, stderr=STDOUT, env=None, sudo=True):
    """Run a shell command subprocess call using Popen.

    :param cmd: (str) Command to run
    :param fatal: (boolean) Whether to raise exception with failure
    :param cwd: (str) Directory path to switch to before running command
    :param stdout: (int) Pass subprocess PIPE if you want to pipe output
           from console
    :param stderr: (int) Pass subprocess PIPE if you want to pipe output
           from console
    :param env: Defines the environment variables for the new process.
    :param sudo: Defines whether command should run in sudo mode
    :return: (dict) The results from subprocess call
    """
    results = {}
    if sudo:
        prefix = "sudo "
        if not cmd.startswith(prefix):
            cmd = prefix + cmd
    proc = Popen(cmd, shell=True, stdout=stdout, stderr=stderr, cwd=cwd, env=env)
    output = proc.communicate()

    results['rc'] = proc.returncode
    results['stdout'] = output[0]
    results['stderr'] = output[0]  # merging both PIPEs by using stderr=STDOUT
    if proc.returncode != 0:
        if fatal:
            raise Exception(results['stderr'])
    return results
