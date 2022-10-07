import datetime
import json
import os
import re
import subprocess
import sys
import time
import zlib
from threading import Timer


class ShCallError(RuntimeError):
  def __init__(self, cmd_str, return_code, additional_message=None):
    msg = f'Error while running "{cmd_str}."'
    if return_code is not None:
      msg += f' Error code: {return_code}'
    if additional_message is not None:
      msg += f' {additional_message}'
    super(ShCallError, self).__init__(msg)
    self.cmd_str = cmd_str
    self.return_code = return_code
    self.message = additional_message

def sh_call(cmd, shell=False, catch_stdout=False, catch_stderr=False, decode=True, split=None, print_output=False,
            expected_return_codes=[0], env=None, cwd=None, timeout=None, verbose=0):
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
    if print_output:
      kwargs['stderr'] = subprocess.STDOUT
    else:
      kwargs['stderr'] = subprocess.PIPE
  if env is not None:
    kwargs['env'] = env
  if cwd is not None:
    kwargs['cwd'] = cwd

  proc = subprocess.Popen(cmd, **kwargs)
  timer = Timer(timeout, proc.kill) if timeout is not None else None
  try:
    if timer is not None:
      timer.start()
    if catch_stdout and print_output:
      output = b''
      err = b''
      for line in proc.stdout:
        output += line
        print(line.decode("utf-8"), end="")
      proc.stdout.close()
      proc.wait()
    else:
      output, err = proc.communicate()
  finally:
    if timer is not None:
      timer.cancel()
  if expected_return_codes is not None and proc.returncode not in expected_return_codes:
    raise ShCallError(cmd_str, proc.returncode)
  if decode:
    if catch_stdout:
      output_decoded = output.decode("utf-8")
      if split is None:
        output = output_decoded
      else:
        output = output_decoded.split(split)
    if catch_stderr:
      err_decoded = err.decode("utf-8")
      if split is None:
        err = err_decoded
      else:
        err = err_decoded.split(split)

  return proc.returncode, output, err

def get_voms_proxy_info():
  _, output, _ = sh_call(['voms-proxy-info'], catch_stdout=True, split='\n')
  info = {}
  for line in output:
    if len(line) == 0: continue
    match = re.match(r'^(.+) : (.+)', line)
    key = match.group(1).strip()
    info[key] = match.group(2)
  if 'timeleft' in info:
    h,m,s = info['timeleft'].split(':')
    info['timeleft'] = float(h) + ( float(m) + float(s) / 60. ) / 60.
  return info

def update_kerberos_ticket(verbose=1):
  sh_call(['kinit', '-R'], verbose=verbose)

def timed_call_wrapper(fn, update_interval, verbose=0):
  last_update = None
  def update(*args, **kwargs):
    nonlocal last_update
    now = datetime.datetime.now()
    delta_t = (now - last_update).total_seconds() if last_update is not None else float("inf")
    if verbose > 0:
      print(f"timed_call for {fn.__name__}: delta_t = {delta_t} seconds")
    if delta_t >= update_interval:
      fn(*args, **kwargs)
      last_update = now
  return update

def adler32sum(file_name):
  block_size = 256 * 1024 * 1024
  asum = 1
  with open(file_name, 'rb') as f:
    while (data := f.read(block_size)):
      asum = zlib.adler32(data, asum)
  return asum

def check_download(local_file, expected_adler32sum=None, raise_exception=False, remote_file=None,
                   remove_bad_file=False):
  if expected_adler32sum is not None:
    asum = adler32sum(local_file)
    if asum != expected_adler32sum:
      if remove_bad_file:
        os.remove(local_file)
      if raise_exception:
        remote_name = remote_file if remote_file is not None else 'file'
        raise RuntimeError(f'Unable to copy {remote_name} from remote. Failed adler32sum check.' + \
                           f' {asum:x} != {expected_adler32sum:x}.')
      return False
  return True


def xrd_copy(input_file_name, local_name, n_retries=4, n_retries_xrdcp=4, n_streams=1, retry_sleep_interval=10,
             expected_adler32sum=None, verbose=1,
             prefixes = [ 'root://cms-xrd-global.cern.ch/', 'root://xrootd-cms.infn.it/',
                          'root://cmsxrootd.fnal.gov/' ]):
  def try_download(prefix):
    try:
      xrdcp_args = ['xrdcp', '--retry', str(n_retries_xrdcp), '--streams', str(n_streams) ]
      if os.path.exists(local_name):
        xrdcp_args.append('--continue')
      if verbose == 0:
        xrdcp_args.append('--silent')
      xrdcp_args.extend([f'{prefix}{input_file_name}', local_name])
      sh_call(xrdcp_args, verbose=1)
      return True
    except ShCallError as e:
        return False

  if os.path.exists(local_name):
    os.remove(local_name)

  for n in range(n_retries):
    for prefix in prefixes:
      if try_download(prefix) and check_download(local_name, expected_adler32sum=expected_adler32sum,
                                                 remove_bad_file=True):
        return
      time.sleep(retry_sleep_interval)

  raise RuntimeError(f'Unable to copy {input_file_name} from remote.')

def webdav_copy(input_remote_file, output_local_file, voms_token, expected_adler32sum=None):
  sh_call(['davix-get', input_remote_file, output_local_file, '-E', voms_token], verbose=1)
  check_download(output_local_file, expected_adler32sum=expected_adler32sum, raise_exception=True,
                 remote_file=input_remote_file)

def gfal_copy(input_remote_file, output_local_file, voms_token, number_of_streams=2, timeout=7200,
              expected_adler32sum=None):
  sh_call(['gfal-copy', '-n', str(number_of_streams), '-t', str(timeout),
           input_remote_file, output_local_file,], shell=False, env={'X509_USER_PROXY': voms_token}, verbose=1)
  check_download(output_local_file, expected_adler32sum=expected_adler32sum, raise_exception=True,
                 remote_file=input_remote_file)

def das_file_site_info(file, verbose=0):
  _, output, _ = sh_call(['dasgoclient', '--json', '--query', f'site file={file}'], catch_stdout=True, verbose=verbose)
  return json.loads(output)

def das_file_pfns(file, disk_only=True, return_adler32=False, verbose=0):
  site_info = das_file_site_info(file, verbose=verbose)
  pfns = []
  adler32 = None
  for entry in site_info:
    if "site" not in entry: continue
    for site in entry["site"]:
      if "pfns" not in site: continue
      for pfns_link, pfns_info in site["pfns"].items():
        if (not disk_only or ("type" in pfns_info and pfns_info["type"] == "DISK")) \
            and pfns_link not in pfns:
          pfns.append(pfns_link)
      if "adler32" in site:
        site_adler32 = int(site["adler32"], 16)
        if adler32 is not None and adler32 != site_adler32:
          raise RuntimeError(f"Inconsistent adler32 sum for {file}")
        adler32 = site_adler32
  if return_adler32:
    return pfns, adler32
  return pfns


def copy_remote_file(input_remote_file, output_local_file, verbose=1):
  pfns_list, adler32 = das_file_pfns(input_remote_file, disk_only=True, return_adler32=True, verbose=verbose)
  if os.path.exists(output_local_file):
    if adler32 is not None and check_download(output_local_file, expected_adler32sum=adler32):
      return
    os.remove(output_local_file)

  if len(pfns_list) == 0:
    raise RuntimeError(f'Unable to find any remote location for {input_remote_file}.')
  for pfns in pfns_list:
    try:
      if verbose > 0:
        print(f"Trying to copy file from {pfns}")
      if pfns.startswith('root:'):
        xrd_copy(pfns, output_local_file, expected_adler32sum=adler32, prefixes=[''], verbose=verbose)
        return
      elif pfns.startswith('davs:'):
        voms_info = get_voms_proxy_info()
        webdav_copy(pfns, output_local_file, voms_info['path'], expected_adler32sum=adler32)
        return
      elif pfns.startswith('srm:') or pfns.startswith('gsiftp'):
        voms_info = get_voms_proxy_info()
        gfal_copy(pfns, output_local_file, voms_info['path'], expected_adler32sum=adler32)
        return
      else:
        print('Skipping an unknown remote source "{pfns}".')
    except (RuntimeError, ShCallError) as e:
      if verbose > 0:
        print(f"Failed to copy file from {pfns}. {e}")

  raise RuntimeError(f'Unable to copy {input_remote_file} from remote.')

if __name__ == "__main__":
  import sys
  cmd = sys.argv[1]
  out = getattr(sys.modules[__name__], cmd)(*sys.argv[2:])
  if out is not None:
    out_t = type(out)
    if out_t in [list, dict]:
      print(json.dumps(out, indent=2))
    else:
      print(out)
