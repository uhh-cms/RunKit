import json
import law
import luigi
import os

from .grid_helper_tasks import CreateVomsProxy

class CrabJob:
    def __init__(self, job_desc, work_area):
        self.name = job_desc["name"]
        self.das_path = job_desc["das_path"]
        self.output = job_desc["output"]
        self.job_area = os.path.join(work_area, self.name)
        self.status = -1
        self.n_resubmit = 0

class CrabTask(law.Task):
    jobs_cfg = luigi.Parameter()
    work_area = luigi.Parameter()

    def requires(self):
        return { "proxy": CreateVomsProxy.req(self) }

    def output(self):
        out = os.path.join(self.work_area, '.prod_done.txt')
        return law.LocalFileTarget(out)

    def run(self):
        with open(self.jobs_cfg, 'r') as f:
            job_descs = json.load(f)
        jobs = []
        for desc in job_descs:
            jobs.append(CrabJob(desc))
        print(f"Progress 0 out of {len(jobs)}.")

