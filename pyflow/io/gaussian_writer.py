from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

from pyflow.flow.flow_utils import load_run_params
from pyflow.io.file_writer import AbstractInputFileWriter


class GaussianWriter(AbstractInputFileWriter):

    @classmethod
    def get_openbabel_format(cls) -> str:
        return "xyz"

    def write(self) -> None:

        # remove charge/multiplicity from OpenBabel generated coordinates
        coordinates = self.coordinates.split("\n", 2)[2]

        # link 0 commands
        if self.args["rwf"] and self.args["chk"]:  # save rwf and chk file
            link0 = "%chk={}.chk\n%rwf={}.rwf\n%Save\n"
        elif self.args["rwf"]:
            link0 = "%chk={}.chk\n%NoSave\n%rwf={}.rwf\n"
        elif self.args["chk"]:
            link0 = "%rwf={}.rwf\n%NoSave\n%chk={}.chk\n"
        else:
            link0 = "%chk={}.chk\n%rwf={}.rwf\n%NoSave\n"

        self.append(link0.format(self.args["title"], self.args["title"]))

        # memory and processor specification
        self.append("%mem={}GB\n%nproc={}\n".format(self.args["memory"], self.args["nproc"]))

        # route, title, charge, and multiplicity
        self.append("{}\n\n".format(self.args["route"]))
        self.append("{}\n\n".format(self.args["title"]))
        self.append("{} {}\n".format(self.args["charge"], self.args["multiplicity"]))

        # coordinates
        self.append("{}\n".format(coordinates))

        if self.args.get("verbose", False):
            print("\n" + self.get_text())

        super().write()


def parse_args(sys_args: List[str]) -> dict:
    # default configuration options
    default_params = load_run_params(program="gaussian16")

    parser = argparse.ArgumentParser(
        description="Gaussian 16 input file options parser",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "-f", "--formats",
        action="store_true",
        default=False,
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

    # Link 0 section
    link0_options = parser.add_argument_group("Link 0 options")
    link0_options.add_argument(
        "-w", "--rwf",
        action="store_true",
        help="save rwf file")
    link0_options.add_argument(
        "-k", "--chk",
        action="store_true",
        help="save chk file")
    link0_options.add_argument(
        "-n", "--nproc",
        type=int,
        help="number of processors",
        default=default_params["nproc"])
    link0_options.add_argument(
        "-m", "--memory",
        type=int,
        help="amount of memory in GB",
        default=default_params["memory"])

    # route section
    route_options = parser.add_argument_group("Route section options")
    route_options.add_argument(
        "-r", "--route",
        type=str,
        help="route for the input file",
        default=argparse.SUPPRESS,
        required="--formats" not in sys.argv and "-f" not in sys.argv)

    # molecule specification
    molecule_options = parser.add_argument_group("Molecule specification")
    molecule_options.add_argument(
        "-c", "--charge",
        type=int,
        help="amount by which to increment the charge of the molecule",
        default=0)
    molecule_options.add_argument(
        "-s", "--multiplicity",
        type=int,
        help="multiplicity of the molecule",
        default=1)
    molecule_options.add_argument(
        "-g", "--geometry_file",
        type=str,
        help="path to geometry file",
        default=argparse.SUPPRESS,
        required="--formats" not in sys.argv and "-f" not in sys.argv)
    molecule_options.add_argument(
        "-gf", "--geometry_format",
        type=str,
        help="the format of the input geometry file")
    molecule_options.add_argument(
        "-sm", "--smiles_geometry_file",
        type=str,
        help="path to geometry file to used to determine the SMILES and charge",
        default=argparse.SUPPRESS)
    molecule_options.add_argument(
        "-smf", "--smiles_geometry_format",
        type=str,
        help="path to geometry file to used to determine the SMILES and charge",
        default=argparse.SUPPRESS)

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
        help="overwrite input file if it already exists")

    # parse arguments
    args = vars(parser.parse_args(sys_args))

    return args


# uses arguments to construct a Gaussian 16 input file
def main(args: dict) -> None:
    if args["title"] is None:
        args["title"] = Path(args["geometry_file"]).stem

    filepath = Path(args.pop("location")).resolve() / "{}.com".format(args["title"])

    gaussian_writer = GaussianWriter(filepath=filepath,
                                     geometry_file=args.pop("geometry_file"),
                                     geometry_format=args.pop("geometry_format"),
                                     overwrite_mode=args.pop("overwrite"),
                                     **args)
    gaussian_writer.write()
