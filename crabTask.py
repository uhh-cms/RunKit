import json
import os
import re
import shutil
import sys

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  sys.path.append(os.path.dirname(file_dir))
  __package__ = 'RunKit'

from .crabTaskStatus import CrabTaskStatus, Status, JobStatus, LogEntryParser
from .sh_tools import ShCallError, sh_call

class Task:
  _taskCfgProperties = [
    'cmsswPython', 'params', 'splitting', 'unitsPerJob', 'scriptExe', 'outputFiles', 'filesToTransfer', 'site',
    'crabOutput', 'localCrabOutput', 'lumiMask', 'maxMemory', 'numCores', 'inputDBS', 'allowNonValid',
    'vomsGroup', 'vomsRole', 'blacklist', 'whitelist', 'dryrun', 'finalOutput', 'maxResubmitCount', 'maxRecoveryCount',
    'targetOutputFileSize', 'ignoreFiles', 'postProcessingDoneFlag',
  ]

  _taskCfgPrivateProperties = [
    'name', 'inputDataset', 'recoveryIndex', 'taskIds',
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
    self.localCrabOutput = ''
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
    self.maxResubmitCount = 0
    self.maxRecoveryCount = 0
    self.finalOutput = ''
    self.targetOutputFileSize = 0
    self.ignoreFiles = []
    self.postProcessingDoneFlag = ''
    self.jobInputFiles = None
    self.datasetFiles = None
    self.fileRunLumi = None
    self.fileRepresentativeRunLumi = None
    self.taskIds = {}

  def checkConfigurationValidity(self):
    def check(cond, prop):
      if not cond:
        raise RuntimeError(f'{self.name}: Configuration error: {prop} is not correctly set.')
    def check_len(prop):
      check(len(getattr(self, prop)) > 0, prop)

    for prop in [ 'cmsswPython', 'splitting', 'outputFiles', 'site', 'crabOutput', 'localCrabOutput', 'inputDBS',
                  'finalOutput', 'name', 'inputDataset' ]:
      check_len(prop)
    check(self.unitsPerJob > 0, 'unitsPerJob')
    check(self.maxMemory > 0, 'maxMemory')
    check(self.numCores > 0, 'numCores')

  def _setFromCfg(self, pName, cfg, add=False):
    if pName in cfg:
      pType = type(getattr(self, pName))
      pValue = cfg[pName]
      if pType != type(pValue):
        raise RuntimeError(f'{self.name}: inconsistent config type for "{pName}". cfg value = "{pValue}"')
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

  def getDatasetFiles(self):
    if self.datasetFiles is None:
      datasetFilesPath = os.path.join(self.workArea, 'dataset_files.json')
      fileRunLumiPath = os.path.join(self.workArea, 'file_run_lumi.json')
      if os.path.exists(datasetFilesPath):
        with open(datasetFilesPath, 'r') as f:
          self.datasetFiles = set(json.load(f))
      else:
        if os.path.exists(fileRunLumiPath):
          self.datasetFiles = set(self.getFileRunLumi().keys())
        else:
          print(f'{self.name}: Gathering dataset files ...')
          _,output,_ = sh_call(['dasgoclient', '--query', f'file dataset={self.inputDataset}'],
                               catch_stdout=True, split='\n')
          self.datasetFiles = set()
          for file in output:
            file = file.strip()
            if len(file) > 0:
              self.datasetFiles.add(file)
        with open(datasetFilesPath, 'w') as f:
          json.dump(list(self.datasetFiles), f, indent=2)
      if len(self.datasetFiles) == 0:
        raise RuntimeError(f'{self.name}: empty dataset {self.inputDataset}')
    return self.datasetFiles

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
      run = str(run)
      if run not in lumiMask:
        lumiMask[run] = []
      lumiMask[run].append([lumi, lumi])
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

  def getTaskOutputPath(self, recoveryIndex=None):
    if recoveryIndex is None:
      recoveryIndex = self.recoveryIndex
    datasetParts = [ s for s in self.inputDataset.split('/') if len(s) > 0 ]
    datasetName = datasetParts[0]
    outputBase = os.path.join(self.localCrabOutput, datasetName)
    return os.path.join(outputBase, 'crab_' + self.requestName(recoveryIndex=recoveryIndex),
                        self.getTaskId(recoveryIndex=recoveryIndex))

  def findOutputFile(self, taskOutput, jobId, outputIndex):
    outputName, outputExt = os.path.splitext(self.outputFiles[outputIndex])
    fileName = f'{outputName}_{jobId}{outputExt}'
    outputFiles = []
    for root, dirs, files in os.walk(taskOutput):
      for file in files:
        if file == fileName:
          filePath = os.path.join(root, file)
          outputFiles.append(filePath)
    if len(outputFiles) == 0:
      raise RuntimeError(f'{self.name}: unable to find output for jobId={jobId} outputName={outputName}' + \
                         f' in {taskOutput}')
    if len(outputFiles) > 1:
      raise RuntimeError(f'{self.name}: duplicated outputs for jobId={jobId} outputName={outputName}' + \
                         f' in {taskOutput}: ' + ' '.join(outputFiles))
    return outputFiles[0]

  def getPostProcessList(self, outputIndex):
    outputFile = self.outputFiles[outputIndex]
    outputName = os.path.splitext(outputFile)[0]
    return os.path.join(self.workArea, f'postProcessList_{outputName}.txt')

  def preparePostProcessList(self, outputIndex):
    listFile = self.getPostProcessList(outputIndex)
    if not os.path.exists(listFile):
      allFiles = self.getDatasetFiles()
      processedFiles = set()
      outputFiles = []
      for recoveryIndex in range(self.recoveryIndex + 1):
        taskOutput = self.getTaskOutputPath(recoveryIndex=recoveryIndex)
        jobIds = self.selectJobIds(JobStatus.finished, recoveryIndex=recoveryIndex)
        jobFilesDict = { job : files for job, files in self.getJobInputFiles(recoveryIndex).items() if job in jobIds }
        for jobId, files in jobFilesDict.items():
          if len(processedFiles.intersection(files)) == 0:
            outputFile = self.findOutputFile(taskOutput, jobId, outputIndex)
            outputFiles.append(outputFile)
            processedFiles.update(files)
      missingFiles = list(allFiles - processedFiles - set(self.ignoreFiles))
      if len(missingFiles) > 0:
        raise RuntimeError(f'{self.name}: missing outputs for following input files: ' + ' '.join(missingFiles))
      with open(listFile, 'w') as f:
        for file in outputFiles:
          f.write(file + '\n')

  def preparePostProcessLists(self):
    for outputIndex in range(len(self.outputFiles)):
      self.preparePostProcessList(outputIndex)

  def getFinalOutput(self):
    return os.path.join(self.finalOutput, self.name)

  def postProcessOutputs(self):
    haddnanoEx_path = os.path.join(os.path.dirname(__file__), 'haddnanoEx.py')
    for outputIndex in range(len(self.outputFiles)):
      outputName = self.outputFiles[outputIndex]
      outputNameBase, outputExt = os.path.splitext(outputName)
      cmd = [ 'python3', '-u', haddnanoEx_path, '--output-dir', self.getFinalOutput(),
              '--output-name', outputName, '--target-size', str(self.targetOutputFileSize),
              '--file-list', self.getPostProcessList(outputIndex) ]
      _, output, _ = sh_call(cmd, catch_stdout=True, catch_stderr=True, print_output=True, verbose=1)
      with open(os.path.join(self.workArea, f'postProcessing_{outputNameBase}.log'), 'w') as f:
        f.write(output)
    self.taskStatus.status = Status.PostProcessingFinished
    self.saveStatus()

  def getPostProcessingDoneFlagFile(self):
    if len(self.postProcessingDoneFlag) == 0:
      raise RuntimeError(f'{self.name}: the post-processing file-flag is not set.')
    return os.path.join(self.workArea, self.postProcessingDoneFlag)

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
    if min_retries[1] >= self.maxResubmitCount:
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
    self.saveStatus()
    return True

  def recover(self):
    filesToProcess = self.getFilesToProcess()
    if len(filesToProcess) == 0:
      print(f'{self.name}: no recovery is needed. All files have been processed.')
      self.taskStatus.status = Status.CrabFinished
      self.saveStatus()
      return True

    if self.recoveryIndex < self.maxRecoveryCount:
      jobIds = self.selectJobIds(JobStatus.finished, invert=True)
      lumiMask = self.getRepresentativeLumiMask(filesToProcess)
      msg = f'{self.name}: creating a recovery task. Attempt {self.recoveryIndex + 1}/{self.maxRecoveryCount}.'
      msg += '\nUnfinished job ids: ' + ', '.join(jobIds)
      msg += '\nFiles to process: ' + ', '.join(filesToProcess)
      msg += '\nRepresentative lumi mask: ' + json.dumps(lumiMask)
      print(msg)
      n_lumi = sum([ len(x) for _, x in lumiMask.items()])
      if n_lumi != len(filesToProcess):
        raise RuntimeError(f"{self.name}: number of representative lumi sections != number of files to process.")
      shutil.copy(self.statusPath, os.path.join(self.workArea, f'status_{self.recoveryIndex}.json'))
      self.recoveryIndex += 1
      self.jobInputFiles = None
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
      self.taskStatus.status = Status.Failed
      self.saveStatus()
      return False

  def recoverLocal(self):
    print(f'{self.name}: local recovery is not implemented.')
    self.taskStatus.status = Status.Failed
    self.saveStatus()
    return False

  def kill(self):
    sh_call(['crab', 'kill', '-d', self.crabArea()])

  def getFilesToProcess(self, lastRecoveryIndex=None, includeNotFinishedFromLastIteration=True):
    allFiles = self.getDatasetFiles()
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
        if len(processedFiles.intersection(files)) == 0:
          processedFiles.update(files)
    return list(allFiles - processedFiles - set(self.ignoreFiles))

  def checkCompleteness(self, includeNotFinishedFromLastIteration=True):
    def file_set(d):
      all_files = set()
      for key, files in d.items():
        for file in files:
          if file in all_files:
            raise RuntimeError(f'Duplicated file entry "{file}"')
          all_files.add(file)
      return all_files

    filesToProcess = self.getFilesToProcess(includeNotFinishedFromLastIteration=includeNotFinishedFromLastIteration)
    if len(filesToProcess):
      print(f'{self.name}: task is not complete. The following files still needs to be processed: {filesToProcess}')
      return False

    processedFiles = set()

    for recoveryIndex in range(self.recoveryIndex + 1):
      jobFilesDict = self.getJobInputFiles(recoveryIndex)
      jobFiles = file_set(jobFilesDict)

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
    taskName = self.name
    customTask = type(taskCfg[taskName]) == dict
    for pName in Task._taskCfgProperties:
      self._setFromCfg(pName, mainCfg, add=False)
      if 'config' in taskCfg:
        self._setFromCfg(pName, taskCfg['config'], add=True)
      if customTask:
        self._setFromCfg(pName, taskCfg[taskName], add=True)
    if customTask:
      inputDataset = taskCfg[taskName]['inputDataset']
    else:
      inputDataset = taskCfg[taskName]
    if inputDataset != self.inputDataset:
      raise RuntimeError(f'{self.name}: change of input dataset is not possible')

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
    customTask = type(taskCfg[taskName]) == dict
    for pName in Task._taskCfgProperties:
      task._setFromCfg(pName, mainCfg, add=False)
      if 'config' in taskCfg:
        task._setFromCfg(pName, taskCfg['config'], add=True)
      if customTask:
        task._setFromCfg(pName, taskCfg[taskName], add=True)
    task.taskStatus.status = Status.Defined
    if customTask:
      task.inputDataset = taskCfg[taskName]['inputDataset']
    else:
      task.inputDataset = taskCfg[taskName]
    task.name = taskName
    task.saveCfg()
    task.saveStatus()
    return task

if __name__ == "__main__":
  import sys

  workArea = sys.argv[1]
  task = Task.Load(workArea=workArea)

  # ok = "OK" if task.checkCompleteness(includeNotFinishedFromLastIteration=False) else "INCOMPLETE"
  # print(f'{task.name}: {ok}')
  # print(task.getAllOutputPaths())
  filesToProcess = task.getFilesToProcess()
  print(f'{task.name}: {len(filesToProcess)} {filesToProcess}')
  lumiMask = task.getRepresentativeLumiMask(filesToProcess)
  n_lumi = sum([ len(x) for _, x in lumiMask.items()])
  print(f'{task.name}: {n_lumi} {lumiMask}')