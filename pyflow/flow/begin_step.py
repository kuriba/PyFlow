import sys
from pathlib import Path

from pyflow.flow.flow_config import FlowConfig
from pyflow.flow.flow_runner import FlowRunner
from pyflow.flow.flow_tracker import FlowTracker
from pyflow.flow.flow_utils import load_workflow_params, WORKFLOW_PARAMS_FILENAME
from pyflow.io.io_utils import upsearch


def begin_step(step_id: str = None, show_progress: bool = False, do_not_track: bool = False) -> None:
    """
    Starts running the specified workflow step.

    :param step_id: the ID of the step to start running
    :param show_progress: displays command-line progress bar if True, no progress bar otherwise
    :param do_not_track: if True, does not track the workflow in the tracked_workflows.csv file
    :return: None
    """

    # try to find workflow .params file
    workflow_params_file = upsearch(WORKFLOW_PARAMS_FILENAME,
                                    message="Please execute this script in a workflow directory.")

    # read config_file and config_id from .params file
    workflow_params = load_workflow_params()
    workflow_main_dir = workflow_params_file.parent
    workflow_id = workflow_main_dir.name
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
        if not do_not_track:
            try:
                FlowTracker.track_new_flow(config_file=config_file,
                                           config_id=config_id,
                                           workflow_main_dir=workflow_main_dir)
            except ValueError as e:
                do_not_track_msg = "Note: if you would like to avoid tracking this workflow," \
                                   " add the --do_not_track flag when you run 'pyflow begin'"
                print("Workflow error: {}\n{}".format(e, do_not_track_msg))
                sys.exit(1)
        show_progress = True
    else:
        FlowTracker.update_progress(workflow_id)

    # setup and start running workflow
    flow_runner = FlowRunner(flow_config=flow_config,
                             current_step_id=step_id,
                             workflow_dir=workflow_main_dir)

    flow_runner.run(show_progress=show_progress)


"""
def initial_setup(flow_config: FlowConfig, workflow_params: dict, workflow_params_file: Path) -> None:

    # add conformer information to .params file
    has_conformers = flow_config.get_step(flow_config.get_initial_step_id())["conformers"]
    if has_conformers:
        num_conformers = int(input("How many conformers does each molecule have?\n"))
    else:
        num_conformers = 1

    workflow_params["num_conformers"] = num_conformers
    with workflow_params_file.open("w") as f:
        f.write(json.dumps(workflow_params, indent=4))
"""
