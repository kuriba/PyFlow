import json
from datetime import datetime
from getpass import getuser
from pathlib import Path

from pyflow.flow.flow_config import FlowConfig
from pyflow.flow.flow_runner import FlowRunner
from pyflow.flow.flow_tracker import FlowTracker
from pyflow.flow.flow_utils import load_workflow_params, WORKFLOW_PARAMS_FILENAME
from pyflow.io.io_utils import upsearch


def begin_step(step_id: str = None, show_progress: bool = False, do_not_track: bool = False):
    # try to find workflow .params file
    workflow_params_file = upsearch(WORKFLOW_PARAMS_FILENAME,
                                    message="Please execute this script in a workflow directory.")

    # read config_file and config_id from .params file
    workflow_params = load_workflow_params()
    workflow_main_dir = workflow_params_file.parent
    config_file = Path(workflow_params["config_file"])
    config_id = workflow_params["config_id"]
    flow_config = FlowConfig(config_file, config_id)

    # validate step_id
    if step_id is None:
        step_id = flow_config.get_initial_step_id()
    elif step_id not in flow_config.get_step_ids():
        message = "Flow config defined in {} does not have a step '{}'".format(config_file, step_id)
        raise AttributeError(message)

    # do stuff on first step (tracking, workflow params modification)
    if flow_config.get_initial_step_id() == step_id:
        initial_setup(flow_config=flow_config,
                      workflow_params=workflow_params,
                      workflow_params_file=workflow_params_file)
        if not do_not_track:
            track_workflow(workflow_params=workflow_params,
                           workflow_main_dir=workflow_main_dir)
        show_progress = True

    # setup and start running workflow
    flow_runner = FlowRunner(flow_config=flow_config,
                             current_step_id=step_id,
                             workflow_dir=workflow_main_dir)

    flow_runner.run(show_progress=show_progress)


def initial_setup(flow_config: FlowConfig, workflow_params: dict, workflow_params_file: Path) -> None:
    """
    Runs initial setup for workflow.

    :param flow_config:
    :param workflow_params:
    :param workflow_params_file:
    :return: None
    """

    # add conformer information to .params file
    has_conformers = flow_config.get_step(flow_config.get_initial_step_id())["conformers"]
    if has_conformers:
        num_conformers = int(input("How many conformers does each molecule have?\n"))
    else:
        num_conformers = 1

    workflow_params["num_conformers"] = num_conformers
    with workflow_params_file.open("w") as f:
        f.write(json.dumps(workflow_params, indent=4))


def track_workflow(workflow_params: dict, workflow_main_dir: Path) -> None:
    """
    Adds the current workflow to the tracked workflows CSV file.

    :param workflow_params:
    :param workflow_main_dir:
    :return: None
    """

    config_file = Path(workflow_params["config_file"])
    config_id = workflow_params["config_id"]

    # workflow tracking
    print("Tracking workflow...")
    workflow_id = workflow_main_dir.name
    flow_tracker = FlowTracker(workflow_id=workflow_id)

    new_flow_info = {"config_file": config_file.as_posix(),
                     "config_id": config_id,
                     "user": getuser(),
                     "run_directory": workflow_main_dir.as_posix(),
                     "submission_date": datetime.today().strftime("%d-%m-%Y"),
                     "submission_time": datetime.today().strftime("%H:%M:%S")}

    flow_tracker.track_new_flow(**new_flow_info)
