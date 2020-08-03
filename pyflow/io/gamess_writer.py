from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyflow.flow.flow_utils import load_run_params
from pyflow.io.file_writer import AbstractInputFileWriter


# script for creating GAMESS input files

class GamessWriter(AbstractInputFileWriter):

    @classmethod
    def get_openbabel_format(self) -> str:
        return "inp"

    def write(self) -> None:

        # reformat coordinates
        coordinates = "\n".join([" $DATA", self.args["title"], self.coordinates.split("\n", 4)[4]])

        # $CONTRL group
        control_group = [" $CONTRL"]

        if self.args["runtyp"] is not None:
            control_group.append("RUNTYP={}".format(self.args["runtyp"]))
        if self.args["dfttyp"] is not None:
            control_group.append("DFTTYP={}".format(self.args["dfttyp"]))
        if self.args["charge"] != 0:
            control_group.append("ICHARG={}".format(self.args["charge"]))
        if self.args["multiplicity"] is not None:
            control_group.append("MULT={}".format(self.args["multiplicity"]))
        if self.args["maxit"] is not None:
            control_group.append("MAXIT={}".format(self.args["maxit"]))

        control_group.append("$END\n")

        if len(control_group) > 2:
            self.append(" ".join(control_group))

        # $SYSTEM group
        system_group = [" $SYSTEM"]
        if self.args["memory"]:
            system_group.append("MWORDS={}".format(self.args["memory"]))
        if self.args["time"]:
            system_group.append("TIMLIM={}".format(self.args["time"]))
        system_group.append("$END\n")
        if len(system_group) > 2:
            self.append(" ".join(system_group))

        # $BASIS group
        basis_group = [" $BASIS"]
        if self.args["gbasis"]:
            basis_group.append("GBASIS={}".format(self.args["gbasis"]))
        basis_group.append("$END\n")
        if len(basis_group) > 2:
            self.append(" ".join(basis_group))

        # $STATPT group
        statpt_group = [" $STATPT"]
        if self.args["opttol"]:
            statpt_group.append("OPTTOL={}".format(self.args["opttol"]))
        if self.args["hess"]:
            statpt_group.append("HESS={}".format(self.args["hess"]))
        if self.args["nstep"]:
            statpt_group.append("NSTEP={}".format(self.args["nstep"]))
        statpt_group.append("$END\n")
        if len(statpt_group) > 2:
            self.append(" ".join(statpt_group))

        # $DFT group
        dft_group = [" $DFT"]
        if self.args["idcver"]:
            dft_group.append("IDCVER={}".format(self.args["idcver"]))
        dft_group.append("$END\n")
        if len(dft_group) > 2:
            self.append(" ".join(dft_group))

        # $DATA group
        self.append(coordinates)

        if self.args.get("verbose", False):
            print("\n" + self.get_text())

        super().write()


def parse_args():
    # default configuration options
    default_params = load_run_params(program="gamess")

    parser = argparse.ArgumentParser(
        description="GAMESS input file options parser",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "-f", "--formats",
        action="store_true",
        help="display supported geometry file formats")

    # general options
    general_group = parser.add_argument_group("General options")
    general_group.add_argument(
        "-t", "--title",
        type=str,
        help="desired name displayed in the title line and used as the filename")
    general_group.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="output input file text to command line")

    # $CONTRL section
    contrl_group = parser.add_argument_group("$CONTRL group")
    contrl_group.add_argument(
        "-c", "--charge",
        type=int,
        default=0,
        help="amount by which to increment the charge of the molecule")
    contrl_group.add_argument(
        "-s", "--multiplicity",
        type=int,
        help="multiplicity of the molecule")
    contrl_group.add_argument(
        "-r", "--runtyp",
        type=str,
        help="the type of computation (e.g., energy, gradient, etc.)")
    contrl_group.add_argument(
        "-d", "--dfttyp",
        type=str,
        help="DFT functional to use (ab initio if unspecified)")
    contrl_group.add_argument(
        "-m", "--maxit",
        type=int,
        help="maximum number of SCF iteration cycles")

    # $SYSTEM section
    system_group = parser.add_argument_group("$SYSTEM group")
    system_group.add_argument(
        "-w", "--memory",
        type=int,
        default=default_params["memory"],
        help="maximum replicated memory which the job can use, on every core \
              given in units of 1,000,000 words (MWORDs) where a word is \
              defined as 64 bits; 1 MWORD = 0.008 GB")

    system_group.add_argument(
        "--time",
        type=int,
        default=default_params["time"],
        help="time limit, in minutes")

    # $BASIS section
    basis_group = parser.add_argument_group("$BASIS group")
    basis_group.add_argument(
        "-b", "--gbasis",
        type=str,
        help="Gaussian basis set specification")

    # $STATPT section
    statpt_group = parser.add_argument_group("$STATPT group")
    statpt_group.add_argument(
        "-op", "--opttol",
        type=float,
        help="gradient convergence tolerance, in Hartree/Bohr")
    statpt_group.add_argument(
        "--hess",
        type=str,
        help="selects the initial hessian matrix")
    statpt_group.add_argument(
        "-n", "--nstep",
        type=int,
        help="maximum number of steps to take")

    # $DFT section
    dft_group = parser.add_argument_group("$DFT group")
    dft_group.add_argument(
        "-id", "--idcver",
        type=int,
        help="the dispersion correction implementation to use")

    # $DATA section
    data_group = parser.add_argument_group("$DATA group")
    data_group.add_argument(
        "-g", "--geometry_file",
        type=str,
        help="path to geometry file",
        required="--formats" not in sys.argv and "-f" not in sys.argv)
    data_group.add_argument(
        "-gf", "--geometry_format",
        type=str,
        help="the format of the input geometry file")

    # file management options
    file_group = parser.add_argument_group("File management options")
    file_group.add_argument(
        "-l", "--location",
        type=str,
        help="directory to save the created file",
        default=".")
    file_group.add_argument(
        "-o", "--overwrite",
        action="store_true",
        help="overwrite input file if exists")

    # parse arguments
    args = vars(parser.parse_args())

    return args


# uses arguments to construct a GAMESS input file
def main(args: dict) -> None:
    if args["title"] is None:
        args["title"] = Path(args["geometry_file"]).stem

    filepath = Path(args.pop("location")).resolve() / "{}.inp".format(args["title"])

    gamess_writer = GamessWriter(geometry_file=args.pop("geometry_file"),
                                 geometry_format=args.pop("geometry_format"),
                                 filepath=filepath,
                                 overwrite=args.pop("overwrite"),
                                 **args)
    gamess_writer.write()


if __name__ == "__main__":
    # parse args
    args = parse_args()

    # generate input file
    main(args)
