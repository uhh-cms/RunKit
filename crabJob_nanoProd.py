import shutil
import os
import yaml

from sh_tools import sh_call

def getFilePath(file):
  if os.path.exists(file):
    return file
  file_name = os.path.basename(file)
  if os.path.exists(file_name):
    return file_name
  if 'ANALYSIS_PATH' in os.environ:
    file_path = os.path.join(os.environ['ANALYSIS_PATH'], file)
    if os.path.exists(file_path):
      return file_path
  if 'CMSSW_BASE' in os.environ:
    file_path = os.path.join(os.environ['CMSSW_BASE'], 'src', file)
    if os.path.exists(file_path):
      return file_path
  raise RuntimeError(f"Unable to find {file}")

def processFile(input_file, output_file, tmp_files, cmssw_report, cmd_line_args, cfg_params):
  run_cmsDriver = True
  store_failed = cfg_params.storeFailed
  skim_cfg = cfg_params.skimCfg
  run_skim = len(skim_cfg) > 0
  debug = len(cmd_line_args) > 0 and cmd_line_args[0] == 'DEBUG'

  if debug:
    if len(cmd_line_args) > 1:
      run_cmsDriver = cmd_line_args[1] == 'True'
    if len(cmd_line_args) > 2:
      run_skim = cmd_line_args[2] == 'True'
    if len(cmd_line_args) > 3:
      store_failed = cmd_line_args[3] == 'True'

  output_name, output_ext = os.path.splitext(output_file)
  cmsDriver_out = output_name + '_cmsDriver' + output_ext
  cmsDriver_py = 'nano_NANO.py'
  tmp_files.extend([ cmsDriver_out, cmsDriver_py ])

  if run_cmsDriver:
    n_threads = 1
    cmsDrive_cmd = [
      'cmsDriver.py', 'nano', '--filein', input_file, '--fileout', f'file:{output_file}',
      '--eventcontent', 'NANOAODSIM', '--datatier', 'NANOAODSIM', '--step', 'NANO', '--nThreads', f'{n_threads}',
      f'--{cfg_params.sampleType}', '--conditions', cfg_params.cond,
      '--era', f"{cfg_params.era}", '-n', f'{cfg_params.maxEvents}', '--no_exec',
    ]

    customise = cfg_params.customisationFunction
    if len(customise) > 0:
      cmsDrive_cmd.extend(['--customise', customise])

    customise_commands = cfg_params.customisationCommands
    if len(customise_commands) > 0:
      cmsDrive_cmd.extend(['--customise_commands', customise_commands])

    cmssw_cmd = [ 'cmsRun',  '-j', cmssw_report, cmsDriver_py ]

    sh_call(cmsDrive_cmd, verbose=1)
    sh_call(cmssw_cmd, verbose=1)

    shutil.move(output_file, cmsDriver_out)


  skim_tree_path = os.path.join(os.path.dirname(__file__), 'skim_tree.py')
  if run_skim:
    with open(skim_cfg, 'r') as f:
      skim_config = yaml.safe_load(f)

    cmd_line = ['python3', skim_tree_path, '--input', cmsDriver_out, '--output', output_file,
                '--input-tree', 'Events', '--other-trees', 'LuminosityBlocks,Runs', '--verbose', '1']

    if 'selection' in skim_config:
      selection = skim_config['selection']
      cmd_line.extend(['--sel', selection])

    if 'processing_module' in skim_config:
      proc_module = skim_config['processing_module']
      cmd_line.extend(['--processing-module', getFilePath(proc_module['file']) + ':' + proc_module['function']])

    if 'column_filters' in skim_config:
      columns = ','.join(skim_config['column_filters'])
      cmd_line.extend([f'--column-filters', columns])

    sh_call(cmd_line, verbose=1)

    if store_failed:
      cmd_line = ['python3', skim_tree_path, '--input', cmsDriver_out, '--output', output_file,
                  '--input-tree', 'Events', '--output-tree', 'EventsNotSelected', '--update-output', '--verbose', '1']

      if 'selection' in skim_config:
        cmd_line.extend(['--invert-sel', '--sel', selection])

      if 'processing_module_for_failed' in skim_config:
        proc_module = skim_config['processing_module_for_failed']
        cmd_line.extend(['--processing-module', getFilePath(proc_module['file']) + ':' + proc_module['function']])

      if 'column_filters_for_failed' in skim_config:
        columns = ','.join(skim_config['column_filters_for_failed'])
        cmd_line.extend([f'--column-filters', columns])

      sh_call(cmd_line, verbose=1)
