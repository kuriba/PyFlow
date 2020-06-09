# TODO write begin_flow:
import json
from datetime import datetime
from getpass import getuser
from pathlib import Path

from pyflow.flow.flow_config import FlowConfig
from pyflow.flow.flow_runner import FlowRunner
from pyflow.flow.flow_tracker import FlowTracker
from pyflow.flow.flow_utils import load_workflow_params, WORKFLOW_PARAMS_FILENAME
from pyflow.io.io_utils import upsearch


def main():
    # try to find workflow .params file
    workflow_config_file = upsearch(WORKFLOW_PARAMS_FILENAME,
                                    message="Please execute this script in a workflow directory.")

    # read config_file and config_id from .params file
    workflow_params = load_workflow_params()

    workflow_main_dir = workflow_config_file.parent
    workflow_id = workflow_main_dir.name
    config_file = Path(workflow_params["config_file"])
    config_id = workflow_params["config_id"]
    flow_config = FlowConfig(config_file, config_id)

    # add conformer information to .params file
    has_conformers = flow_config.get_step(flow_config.get_initial_step_id())["conformers"]
    if has_conformers:
        num_conformers = int(input("How many conformers does each molecule have?\n"))
    else:
        num_conformers = 1

    workflow_params["num_conformers"] = num_conformers
    with workflow_config_file.open("w") as f:
        f.write(json.dumps(workflow_params, indent=4))

    # workflow tracking
    print("Tracking workflow...")
    flow_tracker = FlowTracker(workflow_id=workflow_id)

    new_flow_info = {"config_file": config_file.as_posix(),
                     "config_id": config_id,
                     "user": getuser(),
                     "run_directory": workflow_main_dir.as_posix(),
                     "submission_date": datetime.today().strftime("%d-%m-%Y"),
                     "submission_time": datetime.today().strftime("%H:%M:%S")}

    # TODO uncomment flow_tracker.track_new_flow(**new_flow_info)

    # setup and start running workflow
    flow_runner = FlowRunner(flow_config=flow_config,
                             current_step_id=flow_config.get_initial_step_id(),
                             workflow_dir=workflow_main_dir)
    flow_runner.run(show_progress=True)


if __name__ == "__main__":
    main()
