#from http.client import HTTPException
import traceback
import sys

from crabTask import Task

from CRABClient.UserUtilities import ClientException
from CRABClient.UserUtilities import config as Config
from CRABClient.UserUtilities import getUsernameFromCRIC as getUsername
from CRABAPI.RawCommand import crabCommand

def submit(task: Task):

  config = Config()

  config.General.workArea = task.workArea
  # config.section_("Debug")
  # config.Debug.extraJDL = ["+CMS_ALLOW_OVERFLOW=False"]

  config.JobType.pluginName = 'Analysis'
  config.JobType.psetName = task.cmsswPython
  config.JobType.maxMemoryMB = task.maxMemory
  config.JobType.numCores = task.numCores
  config.JobType.sendPythonFolder = True

  if len(task.scriptExe) > 0:
    config.JobType.scriptExe = task.scriptExe
  config.JobType.inputFiles = task.filesToTransfer
  if len(task.outputFiles) is not None:
    config.JobType.outputFiles = task.outputFiles

  config.Data.inputDBS = task.inputDBS
  config.Data.allowNonValidInputDataset = task.allowNonValid
  config.General.transferOutputs = True
  config.General.transferLogs = False
  config.Data.publication = False

  config.Site.storageSite = task.site

  if len(task.vomsGroup) != 0:
    config.User.voGroup = task.vomsGroup
  if len(task.vomsRole) != 0:
    config.User.voRole = task.vomsRole

  if task.crabOutput[0] == '/':
    config.Data.outLFNDirBase = task.crabOutput
  else:
    config.Data.outLFNDirBase = "/store/user/{}/{}".format(getUsername(), task.crabOutput)

  if len(task.blacklist) != 0:
    config.Site.blacklist = task.blacklist
  if len(task.whitelist) != 0:
    config.Site.whitelist = task.whitelist

  config.JobType.pyCfgParams = task.params
  config.Data.unitsPerJob = task.unitsPerJob
  config.Data.splitting = task.splitting
  config.Data.lumiMask = task.lumiMask
  config.General.requestName = task.requestName()
  config.Data.inputDataset = task.inputDataset

  try:
    print ("Splitting: {} with {} units per job".format(task.splitting, task.unitsPerJob))
    crabCommand('submit', config=config, dryrun=task.dryrun)
  except:
    print(traceback.format_exc())
    sys.exit(1)
  # except HTTPException as hte:
  #   print(str(hte))
  #   print("\n{}\nERROR: failed to submit task due to HTTPException.\n{}".format(hte, hte.headers))
  #   sys.exit(1)
  # except ClientException as cle:
  #   print("ERROR: failed to submit task due to ClientException.\n{}".format(cle))
  #   sys.exit(2)
  # except RuntimeError as err:
  #   print ("ERROR:", str(err), file=sys.stderr)
  #   sys.exit(3)

if __name__ == "__main__":
  workArea = sys.argv[1]
  task = Task.Load(workArea=workArea)
  submit(task)