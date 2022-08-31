import os
import shutil

from sh_tools import sh_call

class InputFile:
  def __init__(self, name):
    self.name = name
    self.size = float(os.path.getsize(name)) / (1024 * 1024)

class OutputFile:
  def __init__(self):
    self.name = None
    self.tmp_name = None
    self.expected_size = 0.
    self.input_files = []

  def try_add(self, file, max_size):
    if self.expected_size + file.size > max_size:
      return False
    self.expected_size += file.size
    self.input_files.append(file)
    return True

  def merge(self):
    haddnano_path = os.path.join(os.path.dirname(__file__), 'haddnano.py')
    cmd = ['python3', '-u', haddnano_path, self.tmp_name ] + [ f.name for f in self.input_files ]
    sh_call(cmd, verbose=1)
    self.size = float(os.path.getsize(self.tmp_name)) / (1024 * 1024)

def merge_files(output, target_size, input_dirs):
  input_files = []
  for input_dir in input_dirs:
    for root, dirs, files in os.walk(input_dir):
      for file in files:
        if file.endswith('.root'):
          file_path = os.path.join(root, file)
          input_files.append(InputFile(file_path))
  input_files = sorted(input_files, key=lambda f: -f.size)
  processed_files = set()
  output_files = []
  while len(processed_files) < len(input_files):
    output_file = OutputFile()
    for file in input_files:
      if file not in processed_files and output_file.try_add(file, target_size):
        processed_files.add(file)
    output_files.append(output_file)

  output_tmp = output + '.tmp'
  if os.path.exists(output):
    shutil.rmtree(output)
  if os.path.exists(output_tmp):
    shutil.rmtree(output_tmp)
  if os.path.exists(output + '.root'):
    os.remove(output + '.root')
  output_basename = os.path.basename(output)
  output_dirname = os.path.dirname(output)

  os.makedirs(output_tmp, exist_ok=False)

  for id, file in enumerate(output_files):
    file.name = os.path.join(output, output_basename + f'_{id}.root')
    file.tmp_name = os.path.join(output_tmp, output_basename + f'_{id}.root')

  if len(output_files) == 1:
    output_files[0].name = output + '.root'

  for file in output_files:
    print(f'Merging {len(file.input_files)} input files into {file.name}...')
    file.merge()
    print(f'Done. Expected size = {file.expected_size:.1f} MiB, actual size = {file.size:.1f} MiB.')
  print("Moving merged files into the final location and removing temporary files.")
  if len(output_files) > 1:
    os.makedirs(output)
  for file in output_files:
    shutil.move(file.tmp_name, file.name)
  shutil.rmtree(output_tmp)
  print('All inputs have been merged.')


if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='hadd nano files.')
  parser.add_argument('--output', required=True, type=str,
                      help="Output where merged files will be stored. If the total number of output files is 1," + \
                           " the output will be stored as output.root, otherwise an output directory will be" + \
                           " created and files inside will have name output_N.root")
  parser.add_argument('--target-size', required=False, type=float, default=2*1024.,
                      help="target output file size in MiB")
  parser.add_argument('input_dir', type=str, nargs='+', help="input directories")
  args = parser.parse_args()

  merge_files(args.output, args.target_size, args.input_dir)
