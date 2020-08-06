import json
from pathlib import Path

from pyflow.flow.flow_config import FlowConfig
from pyflow.flow.flow_utils import WORKFLOW_PARAMS_FILENAME


def setup_dirs(save_location: str, workflow_name: str, config_file: str, config_id: str) -> None:
    """
    Sets up the directories for a new workflow run. This function uses the config
    specified in the given ``config_file`` to determine what step directories to
    create. In addition, this function initializes the .params file in the main
    directory of the workflow. The .params file records the path to the config file
    and the config_id.
    :param save_location: the location to setup the new workflow
    :param workflow_name: the name of the workflow
    :param config_file: the config file for the workflow
    :param config_id: the config ID for the workflow
    :return: None
    :raises FileExistsError: if the specified workflow directory already exists
    """

    save_location = Path(save_location)

    main_dir = save_location / workflow_name

    # read/validate config file
    config = FlowConfig(config_file=config_file, config_id=config_id)

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

    # make directory for initial, unoptimized PDB files
    unopt_pdbs = main_dir / "unopt_pdbs"
    unopt_pdbs.mkdir()

    # write config filename and config ID to .params file in workflow directory
    flow_instance_config_file = main_dir / WORKFLOW_PARAMS_FILENAME
    flow_instance_config = {"config_file": str(Path(config_file).resolve()),
                            "config_id": str(config_id),
                            "num_waves": 1}

    with flow_instance_config_file.open("w") as f:
        f.write(json.dumps(flow_instance_config, indent=4))

    print("Successfully set up workflow directory '{}'".format(workflow_name))
