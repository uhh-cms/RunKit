import json
import os
import re
import shutil

from .crabTaskStatus import CrabTaskStatus, Status, JobStatus, LogEntryParser
from .sh_tools import ShCallError, sh_call

class Task:
  _taskCfgProperties = [
    'cmsswPython', 'params', 'splitting', 'unitsPerJob', 'scriptExe', 'outputFiles', 'filesToTransfer', 'site',
    'crabOutput', 'localOutputPrefix', 'lumiMask', 'maxMemory', 'numCores', 'inputDBS', 'allowNonValid',
    'vomsGroup', 'vomsRole', 'blacklist', 'whitelist', 'dryrun', 'finalOutput', 'maxResubmitCount', 'maxRecoveryCount',
    'targetOutputFileSize',
  ]

  _taskCfgPrivateProperties = [
    'name', 'inputDataset', 'recoveryIndex', 'resubmitCount', 'taskIds',
  ]

  inputLumiMaskJsonName = 'inputLumis'

  def __init__(self):
    self.taskStatus = CrabTaskStatus()
    self.workArea = ''
    self.cfgPath = ''
    self.statusPath = ''
    self.name = ''
    self.inputDataset = ''
    self.cmsswPython = ''
    self.params = {}
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
    self.resubmitCount = 0
    self.maxResubmitCount = 0
    self.maxRecoveryCount = 0
    self.finalOutput = ''
    self.targetOutputFileSize = 0
    self.jobInputFiles = None
    self.fileRunLumi = None
    self.fileRepresentativeRunLumi = None
    self.taskIds = {}

  def _setFromCfg(self, pName, cfg, add=False):
    if pName in cfg:
      pType = type(getattr(self, pName))
      pValue = cfg[pName]
      # if pType != type(pValue):
      #   raise RuntimeError(f'Inconsistent config type for "{pName}".')
      if add:
        if pType == list:
          x = list(set(getattr(self, pName) + pValue))
          setattr(self, pName, x)
        elif pType == dict:
          getattr(self, pName).update(pValue)
        else:
          setattr(self, pName, pValue)
      else:
        setattr(self, pName, pValue)

  def saveCfg(self):
    cfg = { }
    for pName in Task._taskCfgPrivateProperties:
      cfg[pName] = getattr(self, pName)
    for pName in Task._taskCfgProperties:
      cfg[pName] = getattr(self, pName)
    with open(self.cfgPath, 'w') as f:
      json.dump(cfg, f, indent=2)

  def saveStatus(self):
    with open(self.statusPath, 'w') as f:
      f.write(self.taskStatus.to_json())

  def requestName(self, recoveryIndex=None):
    name = self.name
    if recoveryIndex is None:
      recoveryIndex = self.recoveryIndex
    if recoveryIndex > 0:
      name += f'_recovery_{recoveryIndex}'
    return name

  def getParams(self):
    return [ f'{key}={value}' for key,value in self.params.items() ]

  def getUnitsPerJob(self):
    if self.recoveryIndex > 0:
      return 1
    return self.unitsPerJob

  def getSplitting(self):
    if self.recoveryIndex > 0:
      return 'FileBased'
    return self.splitting

  def getLumiMask(self):
    if self.recoveryIndex > 0:
      return os.path.join(self.workArea, f'{Task.inputLumiMaskJsonName}_{self.recoveryIndex}.json')
    return self.lumiMask

  def getMaxMemory(self):
    if self.recoveryIndex > 0:
      return max(self.maxMemory, 4000)
    return self.maxMemory

  def getLocalJobArea(self, recoveryIndex=None):
    localArea = os.path.join(self.crabArea(recoveryIndex), 'local')
    if not os.path.exists(localArea):
      print(f'{self.name}: Preparing local job area ...')
      sh_call(['crab', 'preparelocal', '-d', self.crabArea(recoveryIndex), f'--destdir={localArea}'], catch_stdout=True)
    if not os.path.exists(localArea):
      raise RuntimeError(f'{self.name}: unable to prepare local job area')
    return localArea

  def getJobInputFilesTxtPath(self, recoveryIndex=None):
    localJobArea = self.getLocalJobArea(recoveryIndex)
    inputFilesPath = os.path.join(localJobArea, 'job_input_files')
    if not os.path.exists(inputFilesPath):
      os.mkdir(inputFilesPath)
    if len(os.listdir(inputFilesPath)) == 0:
      inputFilesTar = os.path.join(localJobArea, 'input_files.tar.gz')
      sh_call(['tar', 'xf', inputFilesTar, '-C', inputFilesPath], catch_stdout=True, catch_stderr=True)
    return inputFilesPath

  def getJobInputFiles(self, recoveryIndex=None):
    if recoveryIndex is None:
      recoveryIndex = self.recoveryIndex
    if recoveryIndex == self.recoveryIndex and self.jobInputFiles is not None:
      return self.jobInputFiles
    jsonPath = os.path.join(self.crabArea(recoveryIndex), 'job_input_files.json')
    if not os.path.exists(jsonPath):
      txtPath = self.getJobInputFilesTxtPath(recoveryIndex)
      jobInputFiles = {}
      for fileName in os.listdir(txtPath):
        jobIdMatch = re.match(r'^job_input_file_list_(.*)\.txt$', fileName)
        if jobIdMatch is None:
          raise RuntimeError(f'Unable to extract job id from "{jobIdMatch}"')
        jobId = jobIdMatch.group(1)
        with open(os.path.join(txtPath, fileName), 'r') as f:
          jobInputFiles[jobId] = json.load(f)
      with open(jsonPath, 'w') as f:
        json.dump(jobInputFiles, f, indent=2)
    else:
      with open(jsonPath, 'r') as f:
        jobInputFiles = json.load(f)
    if recoveryIndex == self.recoveryIndex:
      self.jobInputFiles = jobInputFiles
    return jobInputFiles

  def getFileRunLumi(self):
    if self.fileRunLumi is None:
      fileRunLumiPath = os.path.join(self.workArea, 'file_run_lumi.json')
      cmdBase = ['dasgoclient', '--query']
      allRuns = f'file,run,lumi dataset={self.inputDataset}'

      def getDasInfo(cmd):
        _,output,_ = sh_call(cmd, catch_stdout=True, split='\n')
        descs = []
        for desc in output:
          desc = desc.strip()
          if len(desc) > 0:
            split_desc = desc.split(' ')
            if len(split_desc) != 3:
              raise RuntimeError(f'Bad file,run,lumi format in "{desc}"')
            descs.append([split_desc[0], json.loads(split_desc[1]), json.loads(split_desc[2])])
        return descs

      if not os.path.exists(fileRunLumiPath):
        print(f'{self.name}: Gathering file->(run,lumi) correspondance ...')
        self.fileRunLumi = {}
        for file, runs, lumis in getDasInfo(cmdBase + [allRuns]):
          if type(lumis) != list:
            raise RuntimeError(f'Unexpected lumis type for "{file}"')
          if type(runs) == int or len(runs) == 1:
            run = runs if type(runs) == int else runs[0]
            self.fileRunLumi[file] = { run: lumis }
          elif len(runs) == 0:
            raise RuntimeError(f'Empty runs for {file}.')
          else:
            self.fileRunLumi[file] = { }
            for run in runs:
              runRequest = allRuns + f' run={run}'
              for runFile, runRuns, runLumis in getDasInfo(cmdBase + [runRequest]):
                if runFile == file:
                  self.fileRunLumi[file][run] = runLumis
                  break
        with open(fileRunLumiPath, 'w') as f:
          json.dump(self.fileRunLumi, f, indent=2)
      else:
        with open(fileRunLumiPath, 'r') as f:
          self.fileRunLumi = json.load(f)
    return self.fileRunLumi

  def getFileRepresentativeRunLumi(self):
    if self.fileRepresentativeRunLumi is None:
      fileRunLumi = self.getFileRunLumi()
      self.fileRepresentativeRunLumi = {}
      for file, fileRuns in fileRunLumi.items():
        def hasOverlaps(run, lumi):
          for otherFile, otherFileRuns in fileRunLumi.items():
            if otherFile != file and run in otherFileRuns and lumi in otherFileRuns[run]:
              return True
          return False
        def findFirstRepresentative():
          for fileRun, runLumis in fileRuns.items():
            for runLumi in runLumis:
              if not hasOverlaps(fileRun, runLumi):
                return (fileRun, runLumi)
          raise RuntimeError(f"Unable to find representative (run, lumi) for {file}")
        self.fileRepresentativeRunLumi[file] = findFirstRepresentative()
    return self.fileRepresentativeRunLumi

  def getRepresentativeLumiMask(self, files):
    lumiMask = {}
    repRunLumi = self.getFileRepresentativeRunLumi()
    for file in files:
      if file not in repRunLumi:
        raise RuntimeError(f'{self.name}: cannot find representative run-lumi for "{file}"')
      run, lumi = repRunLumi[file]
      if run not in lumiMask:
        lumiMask[str(run)] = []
      lumiMask[str(run)].append([lumi, lumi])
    return lumiMask

  def selectJobIds(self, jobStatus, invert=False, recoveryIndex=None):
    jobIds = []
    for jobId, status in self.getTaskStatus(recoveryIndex).get_job_status().items():
      if (status == jobStatus and not invert) or (status != jobStatus and invert):
        jobIds.append(jobId)
    return jobIds

  def getTaskStatus(self, recoveryIndex=None):
    if recoveryIndex is None:
      recoveryIndex = self.recoveryIndex
    if recoveryIndex == self.recoveryIndex:
      return self.taskStatus
    statusPath = os.path.join(self.workArea, f'status_{recoveryIndex}.json')
    with open(statusPath, 'r') as f:
      return CrabTaskStatus.from_json(f.read())

  def getTaskId(self, recoveryIndex=None):
    if recoveryIndex is None:
      recoveryIndex = self.recoveryIndex
    if recoveryIndex not in self.taskIds:
      if recoveryIndex != self.recoveryIndex:
        statusPath = os.path.join(self.workArea, f'status_{recoveryIndex}.json')
        with open(statusPath, 'r') as f:
          taskStatus = CrabTaskStatus.from_json(f.read())
      else:
        taskStatus = self.taskStatus
      self.taskIds[recoveryIndex] = taskStatus.task_id()
      self.saveCfg()
    return self.taskIds[recoveryIndex]

  def getAllOutputPaths(self):
    outputs = []
    datasetParts = [ s for s in self.inputDataset.split('/') if len(s) > 0 ]
    datasetName = datasetParts[0]
    output_base = os.path.join(self.localOutputPrefix + self.crabOutput, datasetName)
    for idx in range(self.recoveryIndex + 1):
      output = os.path.join(output_base, 'crab_' + self.requestName(idx), self.getTaskId(idx))
      if os.path.exists(output):
        outputs.append(output)
      else:
        print(f'{self.name}: empty output "{output}".')
    return outputs

  def getFinalOutput(self):
    return os.path.join(self.finalOutput, self.name)

  def postProcessOutputs(self):
    haddnanoEx_path = os.path.join(os.path.dirname(__file__), 'haddnanoEx.py')
    cmd = [ 'python3', '-u', haddnanoEx_path, '--output', self.getFinalOutput(),
            '--target-size', str(self.targetOutputFileSize) ]
    outputs = self.getAllOutputPaths()
    if len(outputs) == 0:
      raise RuntimeError("No outputs were found")
    cmd += outputs
    _, output, _ = sh_call(cmd, catch_stdout=True, catch_stderr=True, print_output=True, verbose=1)
    with open(os.path.join(self.workArea, 'postProcessing.log'), 'w') as f:
      f.write(output)
    self.taskStatus.status = Status.PostProcessingFinished
    self.saveStatus()

  def hasFailedJobs(self):
    return JobStatus.failed in self.taskStatus.job_stat

  def crabArea(self, recoveryIndex=None):
    return os.path.join(self.workArea, 'crab_' + self.requestName(recoveryIndex))

  def lastCrabStatusLog(self):
    return os.path.join(self.workArea, 'lastCrabStatus.txt')

  def submit(self):
    crabSubmitPath = os.path.join(os.path.dirname(__file__), 'crabSubmit.py')
    if self.recoveryIndex == 0:
      print(f'{self.name}: submitting ...')
    try:
      sh_call(['python3', crabSubmitPath, self.workArea])
      self.taskStatus.status = Status.Submitted
      self.saveStatus()
    except ShCallError as e:
      crabArea = self.crabArea()
      if os.path.exists(crabArea):
        shutil.rmtree(crabArea)
      raise e

  def updateStatus(self):
    returncode, output, err = sh_call(['crab', 'status', '--json', '-d', self.crabArea()],
                                      catch_stdout=True, split='\n')
    self.taskStatus = LogEntryParser.Parse(output)
    self.saveStatus()
    with open(self.lastCrabStatusLog(), 'w') as f:
      f.write('\n'.join(output))
    self.getTaskId()

  def resubmit(self):
    retries = self.taskStatus.get_detailed_job_stat('Retries', JobStatus.failed)
    if len(retries) == 0:
      return False
    min_retries = min(retries.items(), key=lambda x: x[1])
    max_retries = max(retries.items(), key=lambda x: x[1])
    self.resubmitCount = min_retries[1]
    if self.resubmitCount >= self.maxResubmitCount:
      return False

    report_str = f'{self.name}: resubmitting {len(retries)} failed jobs.'
    if min_retries[1] == max_retries[1]:
      report_str += f' All failed jobs have previous retries attempts = {min_retries[1]}.'
    else:
      report_str += f' Job {min_retries[0]}/{max_retries[0]} has min/max previous retries attempts'
      report_str += f' = {min_retries[1]}/{max_retries[1]}.'
    report_str += f' The max number of allowed attempts = {self.maxResubmitCount}.'
    print(report_str)
    sh_call(['crab', 'resubmit', '-d', self.crabArea()], catch_stdout=True)
    self.taskStatus.status = Status.InProgress
    self.saveCfg()
    self.saveStatus()
    return True

  def recover(self):
    if self.recoveryIndex < self.maxRecoveryCount:
      filesToProcess = self.getFilesToProcess()
      jobIds = self.selectJobIds(JobStatus.finished, invert=True)
      msg = f'{self.name}: creating a recovery task. Attempt {self.recoveryIndex + 1}/{self.maxRecoveryCount}.'
      msg += ' Unfinished job ids: ' + ', '.join(jobIds) + '.'
      msg += ' Files to process: ' + ', '.join(filesToProcess)
      print(msg)
      lumiMask = self.getRepresentativeLumiMask(filesToProcess)
      shutil.copy(self.statusPath, os.path.join(self.workArea, f'status_{self.recoveryIndex}.json'))
      self.recoveryIndex += 1
      with open(self.getLumiMask(), 'w') as f:
        json.dump(lumiMask, f)
      self.saveCfg()
      try:
        self.submit()
      except ShCallError as e:
        self.recoveryIndex -= 1
        self.saveCfg()
        raise e
      return True
    else:
      self.taskStatus.status = Status.CrabFailed
      self.saveStatus()
      return False

  def recoverLocal(self):
    self.taskStatus.status = Status.Failed
    self.saveStatus()
    return False

  def kill(self):
    sh_call(['crab', 'kill', '-d', self.crabArea()])

  def getFilesToProcess(self, lastRecoveryIndex=None, includeNotFinishedFromLastIteration=True):
    allFiles = self.getFileRunLumi().keys()
    processedFiles = set()
    if lastRecoveryIndex is None:
      lastRecoveryIndex = self.recoveryIndex
    for recoveryIndex in range(lastRecoveryIndex + 1):
      if recoveryIndex != self.recoveryIndex or includeNotFinishedFromLastIteration:
        jobIds = self.selectJobIds(JobStatus.finished, recoveryIndex=recoveryIndex)
        jobFilesDict = { job : files for job, files in self.getJobInputFiles(recoveryIndex).items() if job in jobIds }
      else:
        jobFilesDict = self.getJobInputFiles(recoveryIndex)
      for key, files in jobFilesDict.items():
        processedFiles.update(files)
    return list(allFiles - processedFiles)

  def checkCompleteness(self, includeNotFinishedFromLastIteration=True):
    def file_set(d):
      all_files = set()
      for key, files in d.items():
        for file in files:
          if file in all_files:
            raise RuntimeError(f'Duplicated file entry "{file}"')
          all_files.add(file)
      return all_files

    filesToProcess = self.getFilesToProcess(includeNotFinishedFromLastIteration)
    if len(filesToProcess):
      print(f'{self.name}: task in not complete. The following files still needs to be processed: {filesToProcess}')
      return False

    processedFiles = set()

    for recoveryIndex in range(self.recoveryIndex + 1):
      jobFilesDict = self.getJobInputFiles(recoveryIndex)
      jobFiles = file_set(jobFilesDict)

      print(f'n files for {recoveryIndex} = {len(jobFiles)}, n_proc = {len(processedFiles)}')
      intersection = processedFiles.intersection(jobFiles)
      if len(intersection):
        print(f'{self.name}: Duplicated files for iteration {recoveryIndex}')
        print(f'Input files for iteration {recoveryIndex}: {jobFilesDict}')
        print(f'Files that have been already processed: {intersection}')
        return False

      jobIds = self.selectJobIds(JobStatus.finished, recoveryIndex=recoveryIndex)
      jobFilesDict = { job : files for job,files in self.getJobInputFiles(recoveryIndex).items() if job in jobIds }
      jobFiles = file_set(jobFilesDict)
      processedFiles.update(jobFilesDict)
    return True

  def updateConfig(self, mainCfg, taskCfg):
    for pName in Task._taskCfgProperties:
      self._setFromCfg(pName, mainCfg, add=False)
      self._setFromCfg(pName, taskCfg, add=True)
    self.saveCfg()

  def updateStatusFromFile(self, statusPath=None, not_exists_ok=True):
    if statusPath is None:
      statusPath = self.statusPath
    if os.path.exists(statusPath):
      with open(statusPath, 'r') as f:
        self.taskStatus = CrabTaskStatus.from_json(f.read())
      return True
    if not not_exists_ok:
      raise RuntimeError(f'{self.name}: Unable to update config from "{statusPath}".')

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
    for pName in Task._taskCfgPrivateProperties + Task._taskCfgProperties:
      task._setFromCfg(pName, cfg, add=False)
    task.updateStatusFromFile()
    return task

  @staticmethod
  def Create(mainWorkArea, mainCfg, taskCfg, taskName):
    task = Task()
    task.workArea = os.path.join(mainWorkArea, taskName)
    task.cfgPath = os.path.join(task.workArea, 'cfg.json')
    task.statusPath = os.path.join(task.workArea, 'status.json')
    if os.path.exists(task.workArea):
      raise RuntimeError(f'Task with name "{taskName}" already exists.')
    os.mkdir(task.workArea)
    for pName in Task._taskCfgProperties:
      task._setFromCfg(pName, mainCfg, add=False)
      task._setFromCfg(pName, taskCfg, add=True)
    task.taskStatus.status = Status.Defined
    task.inputDataset = taskCfg[taskName]
    task.name = taskName
    task.saveCfg()
    task.saveStatus()
    return task

if __name__ == "__main__":
  import sys

  workArea = sys.argv[1]
  task = Task.Load(workArea=workArea)

  #ok = "OK" if task.checkCompleteness(includeNotFinishedFromLastIteration=False) else "INCOMPLETE"
  #print(f'{task.name}: {ok}')
  # print(task.getAllOutputPaths())
  filesToProcess = task.getFilesToProcess(lastRecoveryIndex=2)
  print(f'{task.name}: {len(filesToProcess)} {filesToProcess}')
  lumiMask = task.getRepresentativeLumiMask(filesToProcess)
  n_lumi = sum([ len(x) for _, x in lumiMask.items()])
  print(f'{task.name}: {n_lumi} {lumiMask}')