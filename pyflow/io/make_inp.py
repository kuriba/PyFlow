import os
import sys
import argparse
from pyflow.mol import mol_utils
from pyflow.io.io_utils import FileWriter
from pyflow.flow.flow_utils import load_run_params


# script for creating GAMESS input files

def parse_args():
    # default configuration options
    config = load_run_params("gamess")

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
        "-c", "--icharg",
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
        "-w", "--words",
        type=int,
        default=config["gamess"]["WORDS"],
        help="maximum replicated memory which the job can use, on every core \
              given in units of 1,000,000 words (MWORDs) where a word is \
              defined as 64 bits; 1 MWORD = 0.008 GB")
    system_group.add_argument(
        "--timlim",
        type=int,
        default=1400,
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
        help="select dispersion correction implementation")

    # $DATA section
    data_group = parser.add_argument_group("$DATA group")
    data_group.add_argument(
        "-g", "--geometry",
        type=str,
        help="path to geometry file",
        required="--formats" not in sys.argv and "-f" not in sys.argv)

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


# uses arguments to construct a Gaussian 16 input file
def main(args):
    if args["formats"]:
        for i in mol_utils.get_supported_babel_formats():
            print(i)
        sys.exit()

    # default title if unspecified
    if args["title"] is None:
        args["title"] = os.path.splitext(os.path.basename(args["geometry"]))[0]

    # input file path
    gamess_file = FileWriter(
        "{}.inp".format(args["title"]),
        args["location"],
        overwrite_mode=args["overwrite"])

    # get formatted coordinates
    coordinates = mol_utils.get_formatted_geometry(args["geometry"],
                                                   output_format="inp")
    coordinates = coordinates.split("\n", 2)[2]

    smiles = mol_utils.get_smiles(args["geometry"])
    charge = mol_utils.get_charge(smiles) + args["icharg"]

    # $CONTRL group
    control_group = [" $CONTRL"]

    if args["runtyp"] is not None:
        control_group.append("RUNTYP={}".format(args["runtyp"]))
    if args["dfttyp"] is not None:
        control_group.append("DFTTYP={}".format(args["dfttyp"]))
    if charge != 0:
        control_group.append("ICHARG={}".format(charge))
    if args["maxit"] is not None:
        control_group.append("MAXIT={}".format(args["maxit"]))

    control_group.append("$END\n")

    if len(control_group) > 2:
        gamess_file.append(" ".join(control_group))

    # $SYSTEM group
    system_group = [" $SYSTEM"]
    if args["words"]:
        system_group.append("MWORDS={}".format(args["words"]))
    if args["timlim"]:
        system_group.append("TIMLIM={}".format(args["timlim"]))
    system_group.append("$END\n")
    if len(system_group) > 2:
        gamess_file.append(" ".join(system_group))

    # $BASIS group
    basis_group = [" $BASIS"]
    if args["gbasis"]:
        basis_group.append("GBASIS={}".format(args["gbasis"]))
    basis_group.append("$END\n")
    if len(basis_group) > 2:
        gamess_file.append(" ".join(basis_group))

    # $STATPT group
    statpt_group = [" $STATPT"]
    if args["opttol"]:
        statpt_group.append("OPTTOL={}".format(args["opttol"]))
    if args["hess"]:
        statpt_group.append("HESS={}".format(args["hess"]))
    if args["nstep"]:
        statpt_group.append("NSTEP={}".format(args["nstep"]))
    statpt_group.append("$END\n")
    if len(statpt_group) > 2:
        gamess_file.append(" ".join(statpt_group))

    # $DFT group
    dft_group = [" $DFT"]
    if args["idcver"]:
        dft_group.append("IDCVER={}".format(args["idcver"]))
    dft_group.append("$END\n")
    if len(dft_group) > 2:
        gamess_file.append(" ".join(dft_group))

    # $DATA group
    gamess_file.append(coordinates)

    if args["verbose"]:
        print("\n" + gamess_file.get_text())

    gamess_file.write()


if __name__ == "__main__":
    # parse args
    args = parse_args()

    # generate input file
    main(args)
