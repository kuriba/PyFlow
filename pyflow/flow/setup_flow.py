import json
from pathlib import Path

from pyflow.flow.flow_config import FlowConfig
from pyflow.flow.flow_utils import WORKFLOW_PARAMS_FILENAME


def setup_dirs(save_location: str, workflow_name: str, config_file: str, config_id: str) -> None:
    """

    :param save_location:
    :param workflow_name:
    :param config_file:
    :param config_id:

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
        sub_dirs = config.get_step_directories(step_id)
        for sub_dir in sub_dirs:
            sub_dir_path = step_dir / sub_dir
            sub_dir_path.mkdir()

    # make directory for initial, unoptimized PDB files
    unopt_pdbs = main_dir / "unopt_pdbs"
    unopt_pdbs.mkdir()

    # write config filename and config ID to .params file in workflow directory
    flow_instance_config_file = main_dir / WORKFLOW_PARAMS_FILENAME
    flow_instance_config = {"config_file": str(Path(config_file).resolve()),
                            "config_id": str(config_id)}

    with flow_instance_config_file.open("w") as f:
        f.write(json.dumps(flow_instance_config, indent=4))

    print("Successfully set up workflow directory '{}'".format(workflow_name))
