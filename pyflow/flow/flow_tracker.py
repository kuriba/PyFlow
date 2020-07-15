import os
from datetime import datetime
from glob import glob
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError

from pyflow.flow.flow_utils import load_workflow_params, WORKFLOW_PARAMS_FILENAME
from pyflow.io.io_utils import upsearch


class FlowTracker:
    """
    Class used for tracking submitted, running, and completed workflows.
    """

    TRACK_FILE = Path("/work/lopez/workflows/tracked_workflows.csv")

    REQUIRED_ATTRIBUTES = ["config_file", "config_id", "user", "run_directory",
                           "submission_date", "submission_time", "progress"]

    def __init__(self, workflow_id: str):
        """
        Constructs a workflow tracker.

        :param workflow_id: the name of the workflow
        """
        self.workflow_id = workflow_id

    def update_progress(self, **kwargs):
        """Updates progress attribute in csv file"""
        # TODO write update method for FlowTracker
        pass

    def track_new_flow(self, **attributes):
        if self.workflow_id_exists():
            raise ValueError(
                "Workflow ID '{}' already exists.".format(self.workflow_id))
        else:
            try:
                tracked_flows = pd.read_csv(FlowTracker.TRACK_FILE, index_col=False)
            except pd.errors.EmptyDataError:
                tracked_flows = pd.DataFrame(columns=FlowTracker.REQUIRED_ATTRIBUTES)

            new_workflow_entry = {"workflow_id": self.workflow_id}
            for attribute, value in attributes.items():
                new_workflow_entry[attribute.strip()] = value

            missing_attributes = []
            for attribute in FlowTracker.REQUIRED_ATTRIBUTES:
                if attribute not in new_workflow_entry:
                    missing_attributes.append(attribute)

            if missing_attributes:
                missing_attributes = ",".join(map(str, missing_attributes))
                raise ValueError("The following attributes are missing: {}".format(
                    missing_attributes))

            tracked_flows = tracked_flows.append(new_workflow_entry, ignore_index=True, sort=False)

            pd.set_option('display.expand_frame_repr', False)

            tracked_flows.to_csv(FlowTracker.TRACK_FILE, index=False)

    def workflow_id_exists(self) -> bool:
        try:
            tracked_workflow_ids = pd.read_csv(FlowTracker.TRACK_FILE)["workflow_id"]
            return self.workflow_id in set(tracked_workflow_ids)
        except pd.errors.EmptyDataError:
            return False

    @staticmethod
    def check_progress(verbose: bool = False) -> float:
        def format_percentage(total: int, percentage: float) -> str:
            percentage_str = "({})".format(round(percentage, 1))
            return "{0:<5} {1:>8}".format(total, percentage_str)

        # ensure user is in a workflow directory
        try:
            workflow_params_file = upsearch(WORKFLOW_PARAMS_FILENAME)
            workflow_dir = workflow_params_file.parent
        except FileNotFoundError:
            msg = "Unable to find workflow directory."
            raise FileNotFoundError(msg)

        from pyflow.flow.flow_config import FlowConfig
        from pyflow.flow.flow_runner import FlowRunner

        workflow_params = load_workflow_params()
        config_file = workflow_params["config_file"]
        config_id = workflow_params["config_id"]

        results_header = ["Step ID", "Completed", "Incomplete", "Running", "Failed"]
        results_table = pd.DataFrame(columns=results_header)

        config = FlowConfig(config_file, config_id)
        for step_id in config.get_step_ids():
            step_config = config.get_step(step_id)

            step_dir = workflow_dir / step_id
            completed_dir = step_dir / "completed"
            failed_dir = step_dir / "failed"
            output_file_ext = FlowRunner.PROGRAM_OUTFILE_EXTENSIONS[step_config["program"]]

            if step_config["conformers"]:
                num_jobs = len(glob(str(workflow_dir / "unopt_pdbs" / "*.pdb")))
            else:
                num_jobs = len(glob(str(workflow_dir / "unopt_pdbs" / "*0.pdb")))

            num_completed = len(glob(str(completed_dir / "*.{}".format(output_file_ext))))
            completion_rate = num_completed / num_jobs

            num_failed = len(glob(str(failed_dir / "*.{}".format(output_file_ext))))
            failure_rate = num_failed / num_jobs

            num_incomplete = num_jobs - num_completed
            incompletion_rate = num_incomplete / num_jobs

            running_jobs = []
            for f in glob(str(step_dir / "*.{}".format(output_file_ext))):
                mtime = datetime.fromtimestamp(os.path.getmtime(f))
                now = datetime.now()

                time_since_mtime = mtime - now
                if time_since_mtime.min < 10:
                    running_jobs.append(f)

            num_running = len(running_jobs)
            running_rate = num_running / num_jobs

            result_entry = {"Step ID": step_id,
                            "Completed": format_percentage(num_completed, completion_rate),
                            "Incomplete": format_percentage(num_incomplete, incompletion_rate),
                            "Running": format_percentage(num_running, running_rate),
                            "Failed": format_percentage(num_failed, failure_rate)}

            results_table.append(result_entry)

        print(results_table.to_string(index=False))

        return 0.
    # TODO mark unchaged workflows
