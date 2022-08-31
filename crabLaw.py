import json
import law
import luigi
import os

from .law_customizations import HTCondorWorkflow
from .crabTask import Task as CrabTask
from .crabTaskStatus import Status

class CrabNanoProdTaskPostProcess(HTCondorWorkflow, law.LocalWorkflow):
  work_area = luigi.Parameter()

  def workflow_requires(self):
    return { }

  def requires(self):
    return {}

  def create_branch_map(self):
    task_list_path = os.path.join(self.work_area, 'tasks.json')
    with open(task_list_path, 'r') as f:
      task_names = json.load(f)
    branches = {}
    n = 0
    for task_name in task_names:
      task = CrabTask.Load(mainWorkArea=self.work_area, taskName=task_name)
      if task.taskStatus.status == Status.CrabFinished:
        branches[n] = task.workArea
        n += 1
    return branches

  def output(self):
    work_area = self.branch_data
    out = os.path.join(work_area, 'post_processing_done.txt')
    return law.LocalFileTarget(out)

  def run(self):
    work_area = self.branch_data
    task = CrabTask.Load(workArea=work_area)
    if task.taskStatus.status != Status.CrabFinished:
      raise RuntimeError(f"task {task.name} is not ready for post-processing")
    print(f'Post-processing {task.name}')
    task.postProcessOutputs()
    self.output().touch()
