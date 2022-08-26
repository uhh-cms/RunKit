import json
import os

import crabTaskStatus as cts
from sh_tools import sh_call

class Task:
  _taskCfgProperties = [
    'cmsswPython', 'params', 'splitting', 'unitsPerJob', 'scriptExe', 'outputFiles', 'filesToTransfer', 'site',
    'crabOutput', 'localOutputPrefix', 'lumiMask', 'maxMemory', 'numCores', 'inputDBS', 'allowNonValid',
    'vomsGroup', 'vomsRole', 'blacklist', 'whitelist', 'dryrun', 'finalOutput',
  ]

  def __init__(self):
    self.taskStatus = cts.CrabTaskStatus()
    self.workArea = ''
    self.cfgPath = ''
    self.statusPath = ''
    self.name = ''
    self.inputDataset = ''
    self.cmsswPython = ''
    self.params = []
    self.splitting = ''
    self.unitsPerJob = -1
    self.scriptExe = ''
    self.outputFiles = []
    self.filesToTransfer = []
    self.site = ''
    self.crabOutput = ''
    self.localOutputPrefix = ''
    self.lumiMask = ''
    self.maxMemory = -1
    self.numCores = -1
    self.inputDBS = ''
    self.allowNonValid = False
    self.vomsGroup = ''
    self.vomsRole = ''
    self.blacklist = []
    self.whitelist = []
    self.dryrun = False
    self.recoveryIndex = 0
    self.finalOutput = ''

  def _setFromCfg(self, pName, cfg):
    if pName in cfg:
      pType = type(getattr(self, pName))
      pValue = cfg[pName]
      if pType != type(pValue):
        raise RuntimeError(f'Inconsistent config type for "{pName}".')
      if pType == list:
        getattr(self, pName).extend(pValue)
      else:
        setattr(self, pName, pValue)

  def saveCfg(self):
    cfg = { 'name': self.name, 'inputDataset': self.inputDataset, 'recoveryIndex': self.recoveryIndex }
    for pName in Task._taskCfgProperties:
      cfg[pName] = getattr(self, pName)
    with open(self.cfgPath, 'w') as f:
      json.dump(cfg, f, indent=2)

  def saveStatus(self):
    with open(self.statusPath, 'w') as f:
      f.write(self.taskStatus.to_json())

  def requestName(self):
    name = self.name
    if self.recoveryIndex > 0:
      name += f'_recovery_{self.recoveryIndex}'
    return name

  def crabArea(self):
    return os.path.join(self.workArea, 'crab_' + self.requestName())

  def submit(self):
    crabSubmitPath = os.path.join(os.path.dirname(__file__), 'crabSubmit.py')
    print(f'Submitting {self.name}...')
    sh_call(['python3', crabSubmitPath, self.workArea])
    self.taskStatus.status = cts.Status.Submitted
    self.saveStatus()

  def updateStatus(self):
    returncode, output, err = sh_call(['crab', 'status', '-d', self.crabArea()], catch_stdout=True, split='\n')
    self.taskStatus = cts.LogEntryParser.Parse(output)
    self.saveStatus()
    log_path = os.path.join(self.workArea, 'lastCrabStatus.txt')
    with open(log_path, 'w') as f:
      f.writelines(output)

  @staticmethod
  def Load(workArea=None, mainWorkArea=None, taskName=None):
    task = Task()
    if (workArea is not None and (mainWorkArea is not None or taskName is not None)) \
        or (mainWorkArea is not None and taskName is None) or (workArea is None and mainWorkArea is None):
      raise RuntimeError("ambiguous Task.Load params")
    if workArea is not None:
      task.workArea = workArea
    else:
      task.name = taskName
      task.workArea = os.path.join(mainWorkArea, taskName)
    task.cfgPath = os.path.join(task.workArea, 'cfg.json')
    task.statusPath = os.path.join(task.workArea, 'status.json')
    with open(task.cfgPath, 'r') as f:
      cfg = json.load(f)
    task.name = cfg['name']
    task.inputDataset = cfg['inputDataset']
    task.recoveryIndex = cfg['recoveryIndex']
    for pName in Task._taskCfgProperties:
      task._setFromCfg(pName, cfg)
    with open(task.statusPath, 'r') as f:
      task.taskStatus = cts.CrabTaskStatus.from_json(f.read())
    return task

  @staticmethod
  def Create(mainWorkArea, mainCfg, taskCfg, taskName):
    task = Task()
    task.name = taskName
    task.workArea = os.path.join(mainWorkArea, taskName)
    task.cfgPath = os.path.join(task.workArea, 'cfg.json')
    task.statusPath = os.path.join(task.workArea, 'status.json')
    if os.path.exists(task.workArea):
      raise RuntimeError(f'Task with name "{taskName}" already exists.')
    os.mkdir(task.workArea)
    for pName in Task._taskCfgProperties:
      task._setFromCfg(pName, mainCfg)
      task._setFromCfg(pName, taskCfg)
    task.taskStatus.status = cts.Status.Defined
    task.inputDataset = taskCfg[taskName]
    task.finalOutput = os.path.join(task.finalOutput, task.name)
    task.saveCfg()
    task.saveStatus()
    return task
