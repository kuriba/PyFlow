import argparse
import json
from pathlib import Path

from pyflow.flow.flow_config import FlowConfig, CONFIG_FILE
from pyflow.flow.flow_utils import get_path_to_pyflow, WORKFLOW_PARAMS_FILENAME


def setup_dirs(args):
    save_location = Path(args["location"])

    main_dir = save_location / args["name"]

    # read/validate config file
    config = FlowConfig(config_file=args["config_file"], config_id=args["config_id"])

    # try to make main workflow directory
    try:
        main_dir.mkdir()
    except FileExistsError:
        message = "The directory {} already exists".format(main_dir.as_posix())
        raise FileExistsError(message)

    # make directories for all of the workflow steps
    for step_id in config.get_step_ids():
        step_dir = main_dir / step_id
        step_dir.mkdir()
        sub_dirs = config.get_step_directories(step_id)
        for sub_dir in sub_dirs:
            sub_dir_path = step_dir / sub_dir
            sub_dir_path.mkdir()

    # make directory for initial, unoptimized PDB files
    unopt_pdbs = main_dir / "unopt_pdbs"
    unopt_pdbs.mkdir()

    # write config filename and config ID to .params file in workflow directory
    flow_instance_config_file = main_dir / WORKFLOW_PARAMS_FILENAME
    flow_instance_config = {"config_file": str(args["config_file"]),
                            "config_id": str(args["config_id"])}

    with flow_instance_config_file.open("w") as f:
        f.write(json.dumps(flow_instance_config, indent=4))


def parse_args():
    parser = argparse.ArgumentParser(
        description="PyFlow workflow directory setup script")

    parser.add_argument(
        "-n", "--name",
        type=str,
        required=True,
        help="the name of the workflow")

    parser.add_argument(
        "-l", "--location",
        type=str,
        default=".",
        help="the location in which to create the workflow directory")

    parser.add_argument(
        "-f", "--config_file",
        type=str,
        default=get_path_to_pyflow() / "conf" / CONFIG_FILE,
        help="the path to the configuration file")

    parser.add_argument(
        "-id", "--config_id",
        type=str,
        default="default",
        help="the id of the desired workflow configuration")

    args = vars(parser.parse_args())

    return args


"""
def main():
    # parse arguments
    args = parse_args()

    # run setup
    setup_dirs(args)


if __name__ == "__main__":
    main()"""
