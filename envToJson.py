import json
import os
import sys

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  sys.path.append(os.path.dirname(file_dir))
  __package__ = 'RunKit'

from .sh_tools import sh_call

def get_env(script, python_cmd='python3'):
  magic_str = '---envToJson.py---'

  cmd = f'{script}; echo {magic_str}; {python_cmd} -c "import json, os; print(json.dumps(dict(os.environ)))"'
  returncode, output, err = sh_call([cmd], shell=True, env={}, catch_stdout=True, split='\n')
  for n, line in enumerate(output):
    if line == magic_str:
      break
  env = json.loads(output[n+1])
  if '_' in env:
    del env['_']
  return env

def get_cmsenv(cmssw_path, python_cmd='python3'):
  script = f'source /cvmfs/cms.cern.ch/cmsset_default.sh; cd {cmssw_path}; eval `scramv1 runtime -sh`'
  return get_env(script, python_cmd=python_cmd)

if __name__ == "__main__":
  script = ' '.join(sys.argv[1:])

  env = get_env(script)
  #env = get_cmsenv(sys.argv[1])
  print(json.dumps(env, indent=2))