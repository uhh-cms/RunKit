import shutil
import sys
import yaml
import PSet
from sh_tools import sh_call, ShCallError

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

def make_job_report(exit_code, exit_message=''):
  if exit_code == 0:
    error_msg = ''
  else:
    error_msg = _error_msg_fmt.format(exit_code, exit_message)
  report_str = _job_report_fmt.format(error_msg)
  with open('FrameworkJobReport.xml.tmp', 'w') as f:
    f.write(report_str)
  shutil.move('FrameworkJobReport.xml.tmp', 'FrameworkJobReport.xml')

def runJob(cmsDriver_out, final_out, run_cmsDriver=True, run_skim=True, store_failed=False):
  if run_cmsDriver:
    p = PSet.process
    input_files = list(p.source.fileNames)

    n_threads = 1
    cmd = [
      'cmsDriver.py', 'nano', '--fileout', f'file:{cmsDriver_out}', '--eventcontent', 'NANOAODSIM',
      '--datatier', 'NANOAODSIM', '--step', 'NANO', '--nThreads', f'{n_threads}',
      #'--customise', 'NanoProd/NanoProd/customize.customize',
      '--filein', ','.join(input_files), f'--{p.exParams.sampleType.value()}',
      '--conditions', p.exParams.cond.value(),
      '--era', f"{p.exParams.era.value()}",
      '-n', f'{p.maxEvents.input.value()}'
    ]

    customise = p.exParams.customisationFunction.value()
    if len(customise) > 0:
      cmd.extend(['--customise', customise])

    sh_call(cmd, verbose=1)

  skim_cfg = 'skim.yaml'
  skim_failed_cfg = 'skim_failed.yaml'
  skim_tree_path = 'skim_tree.py'
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
    with open(skim_failed_cfg, 'r') as f:
      skim_config = yaml.safe_load(f)

    cmd_line = ['python3', skim_tree_path, '--input', cmsDriver_out, '--output', final_out, '--input-tree', 'Events',
                '--output-tree', 'EventsOther', '--update-output', '--verbose', '1']

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


if __name__ == "__main__":
  try:
    if len(sys.argv) > 1:
      cmsDriver_out = sys.argv[1]
      final_out = sys.argv[2]
      run_cmsDriver = sys.argv[3] == 'True'
      run_skim = sys.argv[4] == 'True'
      store_failed = sys.argv[5] == 'True'
    else:
      cmsDriver_out = 'nanoOrig.root'
      final_out = 'nano.root'
      store_failed = False
    runJob(cmsDriver_out, final_out, run_cmsDriver=run_cmsDriver, run_skim=run_skim, store_failed=store_failed)
    make_job_report(0)
  except ShCallError as e:
    print(f'ERROR: {e}')
    make_job_report(e.return_code, str(e))
  # except Exception as e:
  #   print(f'ERROR: {e}')
  #   make_job_report(666, str(e))
  # except:
  #   print(f'UNEXPECTED ERROR')
  #   make_job_report(666, 'Unexpected error')


