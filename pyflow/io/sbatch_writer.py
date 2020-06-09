from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from pyflow.flow.flow_utils import load_run_params
from pyflow.io.file_writer import FileWriter


class SbatchWriter(FileWriter):
    """

    """

    def __init__(self,
                 filepath: Path,
                 commands: str,
                 jobname: str = None,
                 overwrite_mode: bool = False,
                 **kwargs):
        super().__init__(filepath=filepath, overwrite_mode=overwrite_mode)

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

        timelim = step_config.pop("timelim") + step_config.pop("timelim_padding")

        return cls(filepath=filepath,
                   jobname=jobname,
                   timelim=timelim,
                   **step_config,
                   **kwargs)

    def write(self):

        self.append("#!/bin/bash\n")
        self.append("#SBATCH -J {}\n".format(self.jobname))

        self.append("#SBATCH -N {}\n".format(self.args.get("nodes", 1)))

        if self.args["cores"]:
            self.append("#SBATCH -n {}\n".format(self.args["cores"]))

        if self.args["partition"]:
            self.append("#SBATCH -p {}\n".format(self.args["partition"]))

        formatted_time = "{:0>2}:{:0>2}:00".format(*divmod(self.args["time"], 60))
        self.append("#SBATCH --time={}\n".format(formatted_time))

        if self.args["array"]:
            self.append("#SBATCH --array=1-{}%{}\n".format(self.args["array"], self.args["simul_jobs"]))
            self.append("#SBATCH -o %A_%a.o\n#SBATCH -e %A_%a.e\n")

        if self.args["email"]:
            self.append("#SBATCH --mail-user={}\n#SBATCH --mail-type=END\n".format(self.args["email"]))

        self.append("\n" + self.commands + "\n")

        super().write()

    def submit(self) -> int:
        """
        Submits the sbatch file represented by this SbatchWriter using the ``sbatch`` command.
        :return: the job ID of the submitted job
        """
        process = subprocess.run(["sbatch", str(self.filepath.resolve())], capture_output=True, check=True)
        job_id = int(process.stdout.split()[-1])
        return job_id


def parse_args():
    # default configuration options
    config = load_run_params("slurm")

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
        default=config["PARTITION"],
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
        "-a", "--simul_jobs",
        type=int,
        default=config["simul_jobs"],
        help="if setting up an array job, the size of the array")

    parser.add_argument(
        "-e", "--email",
        type=str,
        default=config["EMAIL"],
        help="email address to send job termination notification; default email if none given")

    # script to run
    parser.add_argument(
        "-s", "--script",
        type=int,
        help="script to run",
        default=0)

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

    args = vars(parser.parse_args())

    return args


# uses arguments to construct a Gaussian 16 input file
def main(args):
    # TODO write make_sbatch main(args) func
    pass


if __name__ == "__main__":
    # parse arguments
    args = parse_args()

    # generate input file
    main(args)