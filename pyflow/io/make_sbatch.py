import argparse
from pyflow.flow.flow_utils import load_run_params


# script for creating Slurm submission files

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
        "-N", "--nodes",
        type=int,
        default=1,
        help="number of nodes")

    # optional info
    parser.add_argument(
        "-m", "--memory",
        type=int,
        help="amount of memory in GB")
    parser.add_argument(
        "-e", "--email",
        type=str,
        default=config["EMAIL"],
        help="email address to send job termination \
                              notification; default email if none given")
    parser.add_argument(
        "-n", "--cores",
        type=int,
        help="number of cores")

    # script to run
    parser.add_argument("-s", "--script", type=int, help="script to run",
                        default=0)

    # file management options
    parser.add_argument("-l", "--location", type=str,
                        help="directory to save the created file", default=".")
    parser.add_argument("-o", "--overwrite", action="store_true",
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
