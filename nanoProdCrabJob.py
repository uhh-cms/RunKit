import importlib.util
import os
import shutil
import sys
import traceback
import yaml

from sh_tools import sh_call, ShCallError, copy_remote_file

_error_msg_fmt = '''
<FrameworkError ExitStatus="{}" Type="Fatal error" >
<![CDATA[
{}
]]>
</FrameworkError>'''

_job_report_fmt = '''
<FrameworkJobReport>
<ReadBranches>
</ReadBranches>
<PerformanceReport>
  <PerformanceSummary Metric="StorageStatistics">
    <Metric Name="Parameter-untracked-bool-enabled" Value="true"/>
    <Metric Name="Parameter-untracked-bool-stats" Value="true"/>
    <Metric Name="Parameter-untracked-string-cacheHint" Value="application-only"/>
    <Metric Name="Parameter-untracked-string-readHint" Value="auto-detect"/>
    <Metric Name="ROOT-tfile-read-totalMegabytes" Value="0"/>
    <Metric Name="ROOT-tfile-write-totalMegabytes" Value="0"/>
  </PerformanceSummary>
</PerformanceReport>

<GeneratorInfo>
</GeneratorInfo>
{}
</FrameworkJobReport>
'''

files_to_remove = []

def make_job_report(exit_code, exit_message=''):
  if exit_code == 0:
    error_msg = ''
  else:
    error_msg = _error_msg_fmt.format(exit_code, exit_message)
  report_str = _job_report_fmt.format(error_msg)
  with open('FrameworkJobReport.xml.tmp', 'w') as f:
    f.write(report_str)
  shutil.move('FrameworkJobReport.xml.tmp', 'FrameworkJobReport.xml')

def exit(exit_code, exit_message=''):
  for file in files_to_remove:
    try:
      os.remove(file)
    except:
      pass
  make_job_report(exit_code, exit_message)

def runJob(cmsDriver_out, final_out, run_cmsDriver=True, run_skim=None, store_failed=None):

  pset_path = 'PSet.py'
  if not os.path.exists(pset_path):
    pset_path = os.path.join(os.getenv("CMSSW_BASE"), 'src', 'PSet.py')
  if not os.path.exists(pset_path):
    raise RuntimeError("Cannot find path to PSet.py.")

  spec = importlib.util.spec_from_file_location("PSet", pset_path)
  PSet = importlib.util.module_from_spec(spec)
  sys.modules["PSet"] = PSet
  spec.loader.exec_module(PSet)

  p = PSet.process
  skim_cfg = p.exParams.skimCfg.value()
  if store_failed is None:
    store_failed = p.exParams.storeFailed.value()
  if run_skim is None:
    run_skim = len(skim_cfg) > 0

  if run_cmsDriver:

    n_threads = 1
    cmd_base = [
      'cmsDriver.py', 'nano', '--fileout', f'file:{cmsDriver_out}', '--eventcontent', 'NANOAODSIM',
      '--datatier', 'NANOAODSIM', '--step', 'NANO', '--nThreads', f'{n_threads}',
      f'--{p.exParams.sampleType.value()}', '--conditions', p.exParams.cond.value(),
      '--era', f"{p.exParams.era.value()}", '-n', f'{p.maxEvents.input.value()}'
    ]

    customise = p.exParams.customisationFunction.value()
    if len(customise) > 0:
      cmd_base.extend(['--customise', customise])

    input_remote_files = list(p.source.fileNames)
    success = False
    try_again = False
    exception = None
    try:
      cmd = [ c for c in cmd_base ]
      cmd.extend(['--filein', ','.join(input_remote_files)])
      sh_call(cmd, verbose=1)
      success = True
    except ShCallError as e:
      exception = e
      print(f"cmsRun has failed with exit code = {e.return_code}")
      try_again = e.return_code in [ 65, 84, 85, 92, 8019, 8020, 8021, 8022, 8023, 8028 ]

    if not success and try_again:
      input_files = [ ]
      has_remote_files = False
      for n, remote_file in enumerate(input_remote_files):
        if len(remote_file) == 0:
          raise RuntimeError("Empty input file name.")
        if remote_file.startswith('file:'):
          input_files.append(remote_file)
        else:
          if not has_remote_files:
            print("Copying remote files locally...")
            has_remote_files = True
          local_file = f'inputMiniAOD_{n}.root'
          copy_remote_file(remote_file, local_file, silent=False)
          input_files.append(f'file:{local_file}')
          files_to_remove.append(local_file)

      if has_remote_files:
        print("All file have been copied locally. Trying to run cmsRun the second time.")
        cmd = [ c for c in cmd_base ]
        cmd.extend(['--filein', ','.join(input_files)])
        sh_call(cmd, verbose=1)
        success = True

    if not success:
      raise exception

  skim_tree_path = os.path.join(os.path.dirname(__file__), 'skim_tree.py')
  if run_skim:
    with open(skim_cfg, 'r') as f:
      skim_config = yaml.safe_load(f)

    cmd_line = ['python3', skim_tree_path, '--input', cmsDriver_out, '--output', final_out, '--input-tree', 'Events',
                '--other-trees', 'LuminosityBlocks,Runs', '--verbose', '1']

    if 'selection' in skim_config:
      selection = skim_config['selection']
      cmd_line.extend(['--sel', selection])

    if 'processing_module' in skim_config:
      proc_module = skim_config['processing_module']
      cmd_line.extend(['--processing-module', proc_module['file'] + ':' + proc_module['function']])

    if 'column_filters' in skim_config:
      columns = ','.join(skim_config['column_filters'])
      cmd_line.extend([f'--column-filters', columns])

    sh_call(cmd_line, verbose=1)

    if store_failed:
      cmd_line = ['python3', skim_tree_path, '--input', cmsDriver_out, '--output', final_out, '--input-tree', 'Events',
                  '--output-tree', 'EventsNotSelected', '--update-output', '--verbose', '1']

      if 'selection' in skim_config:
        cmd_line.extend(['--invert-sel', '--sel', selection])

      if 'processing_module_for_failed' in skim_config:
        proc_module = skim_config['processing_module_for_failed']
        cmd_line.extend(['--processing-module', proc_module['file'] + ':' + proc_module['function']])

      if 'column_filters_for_failed' in skim_config:
        columns = ','.join(skim_config['column_filters_for_failed'])
        cmd_line.extend([f'--column-filters', columns])

      sh_call(cmd_line, verbose=1)


if __name__ == "__main__":
  try:
    kwargs = {}
    if len(sys.argv) > 1:
      cmsDriver_out = sys.argv[1]
      final_out = sys.argv[2]
      if len(sys.argv) > 3:
        kwargs['run_cmsDriver'] = sys.argv[3] == 'True'
      if len(sys.argv) > 4:
        kwargs['run_skim'] = sys.argv[4] == 'True'
      if len(sys.argv) > 5:
        kwargs['store_failed'] = sys.argv[5] == 'True'
    else:
      cmsDriver_out = 'nanoOrig.root'
      final_out = 'nano.root'
    runJob(cmsDriver_out, final_out, **kwargs)
    exit(0)
  except ShCallError as e:
    print(traceback.format_exc())
    exit(e.return_code, str(e))
  except Exception as e:
    print(traceback.format_exc())
    exit(666, str(e))
  except:
    print(traceback.format_exc())
    exit(666, 'Unexpected error')


