# Crab wrapper.

import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing

options = VarParsing('analysis')
options.register('cmsRunCfg', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Path to the cmsRun python configuration file.")
options.register('cmsRunOptions', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Comma separated list of options that should be passed to cmsRun.")
options.register('skimCfg', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Skimming configuration in YAML format.")
options.register('skimSetup', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Name of the skim setup for passed events.")
options.register('skimSetupFailed', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Name of the skim setup for failed events.")
options.register('storeFailed', False, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "Store minimal information about events that failed selection.")
options.register('mustProcessAllInputs', False, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "To sucessfully finish, all inputs must be processed.")
options.register('createTar', True, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "Create a tar file with all outputs.")
options.register('maxRuntime', 20, VarParsing.multiplicity.singleton, VarParsing.varType.int,
                 "Maximal expected job runtime in hours.")
options.register('maxFiles', -1, VarParsing.multiplicity.singleton, VarParsing.varType.int,
                 "Maximal number of files to process.")
options.register('customiseCmds', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Production customization commands (if any)")
options.register('writePSet', False, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "Dump configuration into PSet.py.")
options.register('copyInputsToLocal', True, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "Copy inputs (one at the time) to a job working directory before processing them.")
options.register('output', 'nano.root', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Name of the output file.")
options.register('datasetFiles', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 """Path to a JSON file with the dict of all files in the dataset.
                    It is used to assing file ids to the outputs.
                    If empty, indices of input files as specified in inputFiles are used.""")

options.parseArguments()

process = cms.Process('cmsRun')
process.source = cms.Source("PoolSource", fileNames = cms.untracked.vstring(options.inputFiles))
process.options = cms.untracked.PSet(wantSummary = cms.untracked.bool(False))
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))
if options.maxEvents > 0:
  process.maxEvents.input = options.maxEvents
process.exParams = cms.untracked.PSet(
  cmsRunCfg = cms.untracked.string(options.cmsRunCfg),
  cmsRunOptions = cms.untracked.string(options.cmsRunOptions),
  skimCfg = cms.untracked.string(options.skimCfg),
  skimSetup = cms.untracked.string(options.skimSetup),
  skimSetupFailed = cms.untracked.string(options.skimSetupFailed),
  storeFailed = cms.untracked.bool(options.storeFailed),
  customisationCommands = cms.untracked.string(options.customiseCmds),
  createTar = cms.untracked.bool(options.createTar),
  mustProcessAllInputs = cms.untracked.bool(options.mustProcessAllInputs),
  maxRuntime = cms.untracked.int32(options.maxRuntime),
  jobModule = cms.untracked.string('crabJob_cmsRun.py'),
  output = cms.untracked.string(options.output),
  datasetFiles = cms.untracked.string(options.datasetFiles),
  maxFiles = cms.untracked.int32(options.maxFiles),
  copyInputsToLocal = cms.untracked.bool(options.copyInputsToLocal),
)

if options.writePSet:
  with open('PSet.py', 'w') as f:
    print(process.dumpPython(), file=f)