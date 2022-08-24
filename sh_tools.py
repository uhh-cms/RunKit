import os
import subprocess
import sys
import time
import zlib

class ShCallError(RuntimeError):
    def __init__(self, cmd_str, return_code):
        super(ShCallError, self).__init__(f'Error while running "{cmd_str}". Error code: {return_code}')
        self.cmd_str = cmd_str
        self.return_code = return_code

def sh_call(cmd, shell=False, catch_stdout=False, catch_stderr=False, decode=True, split=None,
            expected_return_codes=[0], verbose=0):
    cmd_str = []
    for s in cmd:
        if ' ' in s:
            s = f"'{s}'"
        cmd_str.append(s)
    cmd_str = ' '.join(cmd_str)
    if verbose > 0:
        print(f'>> {cmd_str}', file=sys.stderr)
    kwargs = {
        'shell': shell,
    }
    if catch_stdout:
        kwargs['stdout'] = subprocess.PIPE
    if catch_stderr:
        kwargs['stderr'] = subprocess.PIPE
    proc = subprocess.Popen(cmd, **kwargs)
    output, err = proc.communicate()
    if expected_return_codes is not None and proc.returncode not in expected_return_codes:
        raise ShCallError(cmd_str, proc.returncode)
    if decode:
        if catch_stdout:
            output_decoded = output.decode("utf-8")
            if split is None:
                output = output_decoded
            else:
                output = [ s for s in output_decoded.split(split) ]
        if catch_stderr:
            err_decoded = err.decode("utf-8")
            if split is None:
                err = err_decoded
            else:
                err = [ s for s in err_decoded.split(split) ]

    return proc.returncode, output, err

def adler32sum(file_name):
    block_size = 256 * 1024 * 1024
    asum = 1
    with open(file_name, 'rb') as f:
        while (data := f.read(block_size)):
            asum = zlib.adler32(data, asum)
    return asum

def xrd_copy(input_file_name, local_name, n_retries=4, n_retries_xrdcp=4, n_streams=1, retry_sleep_interval=10,
             expected_adler32sum=None, silent=True,
             prefixes = [ 'root://cms-xrd-global.cern.ch/', 'root://xrootd-cms.infn.it/',
                          'root://cmsxrootd.fnal.gov/' ]):
    def try_download(prefix):
        try:
            xrdcp_args = ['xrdcp', '--retry', str(n_retries_xrdcp), '--streams', str(n_streams) ]
            if os.path.exists(local_name):
                xrdcp_args.append('--continue')
            if silent:
                xrdcp_args.append('--silent')
            xrdcp_args.extend([f'{prefix}{input_file_name}', local_name])
            sh_call(xrdcp_args, verbose=1)
            return True
        except RuntimeError as e:
            return False

    def check_download():
        if expected_adler32sum is not None:
            asum = adler32sum(local_name)
            if asum != expected_adler32sum:
                os.remove(local_name)
                return False
        return True

    if os.path.exists(local_name):
        os.remove(local_name)

    for n in range(n_retries):
        for prefix in prefixes:
            if try_download(prefix) and check_download():
                return
            time.sleep(retry_sleep_interval)

    raise RuntimeError(f'Unable to copy {input_file_name} from remote.')
