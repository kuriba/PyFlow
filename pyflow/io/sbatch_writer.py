from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List

from pyflow.flow.flow_utils import load_run_params
from pyflow.io.file_writer import FileWriter


class SbatchWriter(FileWriter):
    """
    Class for writing Slurm submission scripts
    """

    def __init__(self,
                 filepath: Path,
                 commands: str,
                 jobname: str = None,
                 overwrite: bool = False,
                 **kwargs):
        super().__init__(filepath=filepath, overwrite=overwrite)

        self.jobname = jobname
        self.commands = commands
        self.args = {}
        for k, v in kwargs.items():
            if k not in self.args:
                self.args[k] = v

    @classmethod
    def from_config(cls,
                    step_config: dict,
                    filepath: Path,
                    jobname: str,
                    **kwargs) -> SbatchWriter:

        time = step_config.pop("time") + step_config.pop("time_padding")

        return cls(filepath=filepath,
                   jobname=jobname,
                   time=time,
                   **step_config,
                   **kwargs)

    def write(self):

        self.append("#!/bin/bash\n")
        self.append("#SBATCH -J {}\n".format(self.jobname))

        self.args["output"] = self.args.get("output", "{}.o".format(self.jobname))
        self.append("#SBATCH -o {}\n".format(self.args["output"]))

        self.args["error"] = self.args.get("error", "{}.e".format(self.jobname))
        self.append("#SBATCH -e {}\n".format(self.args["error"]))

        self.append("#SBATCH -N {}\n".format(self.args.get("nodes", 1)))

        if self.args.get("cores"):
            self.append("#SBATCH -n {}\n".format(self.args["cores"]))

        if self.args.get("partition"):
            self.append("#SBATCH -p {}\n".format(self.args["partition"]))

        if self.args.get("time"):
            formatted_time = "{:0>2}:{:0>2}:00".format(*divmod(self.args["time"], 60))
            self.append("#SBATCH --time={}\n".format(formatted_time))

        if self.args.get("array"):
            self.append("#SBATCH --array=1-{}%{}\n".format(self.args["array"], self.args["simul_jobs"]))
            if not self.args.get("output"):
                self.append("#SBATCH -o %A_%a.o\n")
            if not self.args.get("error"):
                self.append("#SBATCH -e %A_%a.e\n")

        if self.args.get("email"):
            self.append("#SBATCH --mail-user={}\n#SBATCH --mail-type=END\n".format(self.args["email"]))

        if self.args.get("dependency_type"):
            self.append("#SBATCH --dependency={}:{}\n".format(self.args["dependency_type"], self.args["dependency_id"]))

        self.append("\n" + self.commands + "\n")

        super().write()

    def submit(self) -> int:
        """
        Submits the sbatch file represented by this SbatchWriter using the ``sbatch`` command.
        :return: the job ID of the submitted job
        """
        working_dir = self.filepath.parent
        process = subprocess.run(["sbatch", str(self.filepath.resolve())],
                                 capture_output=True, check=True, cwd=working_dir)
        job_id = int(process.stdout.split()[-1])
        return job_id


def parse_args(sys_args: List[str]) -> dict:
    # default configuration options
    config = load_run_params(program="slurm")

    parser = argparse.ArgumentParser(
        description="Slurm sbatch file options parser")

    # general info
    parser.add_argument(
        "-j", "--jobname",
        type=str,
        required=True,
        help="name of job submission")
    parser.add_argument(
        "-p", "--partition",
        type=str,
        default=config["partition"],
        help="partition on which to run the job")
    parser.add_argument(
        "-t", "--time",
        type=int,
        default=1440,
        help="time limit in minutes")

    parser.add_argument(
        "-N", "--nodes",
        type=int,
        default=1,
        help="number of nodes")

    parser.add_argument(
        "-n", "--cores",
        type=int,
        help="number of cores")

    # optional info
    parser.add_argument(
        "-m", "--mem",
        type=int,
        help="amount of memory per node in gigabytes")
    parser.add_argument(
        "-mcpu", "--mem-per-cpu",
        type=int,
        help="amount of memory per CPU in gigabytes")
    parser.add_argument(
        "-a", "--array",
        type=int,
        help="if setting up an array job, the size of the array")
    parser.add_argument(
        "--simul_jobs",
        type=int,
        default=config["simul_jobs"],
        help="if setting up an array job, the size of the array")
    parser.add_argument(
        "-e", "--email",
        type=str,
        default=config["email"],
        help="email address to send job termination notification; default email if none given")
    parser.add_argument(
        "-d_type", "--dependency_type",
        type=str,
        help="type of Slurm dependency")
    parser.add_argument(
        "-d_id", "--dependency_id",
        type=int,
        required="dependency_type" in sys.argv,
        help="job ID on which to create the dependency")

    # code to run
    parser.add_argument(
        "-c", "--commands",
        type=str,
        required=True,
        help="file containing commands to run")

    # file management options
    parser.add_argument(
        "-l", "--location",
        type=str,
        default=".",
        help="directory to save the created file")
    parser.add_argument(
        "-o", "--overwrite",
        action="store_true",
        help="overwrite sbatch file if exists")
    parser.add_argument(
        "-u", "--output",
        type=str,
        help="name of Slurm output file")
    parser.add_argument(
        "-r", "--error",
        type=str,
        help="name of Slurm error file")

    args = vars(parser.parse_args(sys_args))

    return args


# uses arguments to construct a Slurm submission script
def main(args: dict):
    sbatch_writer = SbatchWriter(**args)

    sbatch_writer.write()
