import law
import luigi
import math
import os

law.contrib.load("htcondor")

class HTCondorWorkflow(law.htcondor.HTCondorWorkflow):
    max_runtime = law.DurationParameter(default=24.0, unit="h", significant=False,
                                        description="maximum runtime, default unit is hours")
    n_cpus = luigi.IntParameter(default=1, description="number of cpus")
    requirements = luigi.Parameter(default='')
    #requirements = luigi.Parameter(default='( (OpSysAndVer =?= "CentOS7") || (OpSysAndVer =?= "CentOS8") )')
    bootstrap_path = luigi.Parameter()
    log_path = luigi.Parameter()
    sub_dir = luigi.Parameter()

    def htcondor_output_directory(self):
        # the directory where submission meta data should be stored
        return law.LocalDirectoryTarget(os.path.join(self.sub_dir, self.__class__.__name__))

    def htcondor_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        return self.bootstrap_path

    def htcondor_job_config(self, config, job_num, branches):
        config.render_variables["analysis_path"] = os.getenv("ANALYSIS_PATH")
        if len(self.requirements) > 0:
            config.custom_content.append(("requirements", self.requirements))
        config.custom_content.append(("+MaxRuntime", int(math.floor(self.max_runtime * 3600)) - 1))
        config.custom_content.append(("RequestCpus", self.n_cpus))

        log_path = os.path.abspath(self.log_path)
        os.makedirs(log_path, exist_ok=True)
        config.custom_content.append(("log", os.path.join(log_path, 'job.$(ClusterId).$(ProcId).log')))
        config.custom_content.append(("output", os.path.join(log_path, 'job.$(ClusterId).$(ProcId).out')))
        config.custom_content.append(("error", os.path.join(log_path, 'job.$(ClusterId).$(ProcId).err')))
        return config