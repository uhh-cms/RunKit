import json
import os
import shutil
import yaml

import crabTaskStatus as cts
from crabTask import Task

def overseer_main(work_area, cfg_file, new_task_list_files, verbose):
  if not os.path.exists(work_area):
    os.makedirs(work_area)
  cfg_path = os.path.join(work_area, 'cfg.yaml')
  if cfg_file is not None:
    shutil.copyfile(cfg_file, cfg_path)
  if not os.path.isfile(cfg_path):
    raise RuntimeError("The overseer configuration is not found")
  with open(cfg_path, 'r') as f:
    main_cfg = yaml.safe_load(f)

  task_list_path = os.path.join(work_area, 'tasks.json')
  tasks = {}
  if os.path.isfile(task_list_path):
    with open(task_list_path, 'r') as f:
      task_names = json.load(f)
      for task_name in task_names:
        tasks[task_name] = Task.Load(mainWorkArea=work_area, taskName=task_name)
  for task_list_file in new_task_list_files:
    with open(task_list_file, 'r') as f:
      new_tasks = yaml.safe_load(f)
    for task_name in new_tasks:
      if task_name in Task._taskCfgProperties or task_name in tasks: continue
      tasks[task_name] = Task.Create(work_area, main_cfg, new_tasks, task_name)
  with open(task_list_path, 'w') as f:
    json.dump([task_name for task_name in tasks], f, indent=2)
  for task_name, task in tasks.items():
    if task.taskStatus.status == cts.Status.Defined:
      task.submit()
    elif task.taskStatus.status != cts.Status.Finished:
      task.updateStatus()
    print(f'{task_name}: {task.taskStatus.status.name}')

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='CRAB overseer.')
  parser.add_argument('--work-area', required=False, type=str, default='.crabOverseer',
                      help="Working area to store overseer and crab tasks states")
  parser.add_argument('--cfg', required=False, type=str, default=None, help="configuration file")
  parser.add_argument('--verbose', required=False, type=int, default=1, help="verbosity level")
  parser.add_argument('task_file', type=str, nargs='*', help="file(s) with task descriptions")
  args = parser.parse_args()

  overseer_main(args.work_area, args.cfg, args.task_file, args.verbose)
