import json
import sys

txt = sys.argv[1]
with open(txt, 'r') as f:
  files = f.readlines()

lumi_map = {}

for desc in files:
  file, run, lumis_str = desc.split(' ')
  lumis = json.loads(lumis_str)
  for lumi in lumis:
    if lumi in lumi_map:
      raise RuntimeError(f'Duplicated lumi={lumi} in file {file} and {lumi_map[file]}')
    lumi_map[lumi] = file

print("all ok")