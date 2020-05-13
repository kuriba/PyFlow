import os
import sys
import argparse
from pyflow.io.io_utils import FileWriter
from pyflow.mol import mol_utils
from pyflow.flow.flow_utils import load_run_params


def parse_args():
    # default configuration options
    config = load_run_params(program="gaussian16")

    parser = argparse.ArgumentParser(
        description="Gaussian 16 input file options parser",
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
        default=config["NPROC"])
    link0_options.add_argument(
        "-m", "--memory",
        type=int,
        help="amount of memory in GB",
        default=config["MEMORY"])

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
        "-g", "--geometry",
        type=str,
        help="path to geometry file",
        default=argparse.SUPPRESS,
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
        help="overwrite input file if it already exists")

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
    g16_file = FileWriter(
        "{}.com".format(args["title"]),
        args["location"],
        overwrite_mode=args["overwrite"])

    # get formatted coordinates
    coordinates = mol_utils.get_formatted_geometry(args["geometry"])
    coordinates = coordinates.split("\n", 2)[2]

    # title, charge, multiplicity, coordinates
    smiles = mol_utils.get_smiles(args["geometry"])
    charge = mol_utils.get_charge(smiles) + args["charge"]

    # link 0 commands
    if args["rwf"] and args["chk"]:  # save rwf and chk file
        link0 = "%chk={}.chk\n%rwf={}.rwf\n%Save\n"
    elif args["rwf"]:
        link0 = "%chk={}.chk\n%NoSave\n%rwf={}.rwf\n"
    elif args["chk"]:
        link0 = "%rwf={}.rwf\n%NoSave\n%chk={}.chk\n"
    else:
        link0 = "%chk={}.chk\n%rwf={}.rwf\n%NoSave\n"
    g16_file.append(link0.format(args["title"], args["title"]))

    # memory and processor specification
    g16_file.append(
        "%mem={}GB\n%nproc={}\n".format(args["memory"], args["nproc"]))

    # route, title, charge, and multiplicity
    g16_file.append("{}\n\n".format(args["route"]))
    g16_file.append("{}\n\n".format(args["title"]))
    g16_file.append("{} {}\n".format(charge, args["multiplicity"]))

    # coordinates
    g16_file.append("{}".format(coordinates))

    if args["verbose"]:
        print("\n" + g16_file.get_text())

    g16_file.write()


if __name__ == "__main__":
    # parse args
    args = parse_args()

    # generate input file
    main(args)
