import sys
from sh_tools import sh_call

if __name__ == '__main__':
  for dataset in sys.argv[1:]:
    try:
      _, output, _ = sh_call(['dasgoclient', '--query', f'dataset dataset={dataset}'], catch_stdout=True, split='\n')
      output = [ s.strip() for s in output if len(s.strip()) > 0]
      if len(output) == 0:
        print(f'missing: {dataset}')
      if len(output) > 1:
        print(f'duplicates: {dataset}')
      if output[0] != dataset:
        print(f'inconsistent: {dataset}')
      print(f'ok: {dataset}')
    except:
      print(f'query_failed: {dataset}')