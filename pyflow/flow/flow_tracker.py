import os
from datetime import datetime
from getpass import getuser
from glob import glob
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError
from tabulate import tabulate

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

    @staticmethod
    def update_progress(workflow_id: str) -> None:
        """Updates progress attribute in csv file"""

        flow_tracker = FlowTracker(workflow_id)

        current_progress = FlowTracker.check_progress(verbose=False)
        formatted_progress = "{}%".format(current_progress)

        df = pd.read_csv(FlowTracker.TRACK_FILE, index_col=False)
        df.loc[df["workflow_id"] == flow_tracker.workflow_id, "progress"] = formatted_progress
        df.to_csv(FlowTracker.TRACK_FILE, index=False)

    def track_flow(self, **attributes) -> None:
        """
        Tracks the given attributes in the tracked_workflows.csv file.
        :param attributes: the attributes and values to track
        :return: None
        """
        if self.workflow_id_exists():
            raise ValueError("workflow ID '{}' already exists.".format(self.workflow_id))
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
                raise ValueError("the following attributes are missing: {}".format(
                    missing_attributes))

            tracked_flows = tracked_flows.append(new_workflow_entry, ignore_index=True, sort=False)

            pd.set_option('display.expand_frame_repr', False)

            tracked_flows.to_csv(FlowTracker.TRACK_FILE, index=False)

    def workflow_id_exists(self) -> bool:
        """
        Determines if the workflow_id exists in the tracked_workflows.csv file.
        :return: True if the workflow_id exists, False otherwise
        """
        try:
            tracked_workflow_ids = pd.read_csv(FlowTracker.TRACK_FILE)["workflow_id"]
            return self.workflow_id in set(tracked_workflow_ids)
        except pd.errors.EmptyDataError:
            return False

    @staticmethod
    def check_progress(verbose: bool = True) -> float:
        """
        Checks the progress of the current workflow directory and prints a progress
        report to the command line (if ``verbose == True``). Returns a float representing
        the completion rate for the workflow (calculated as the quotient of the total
        number of completed calculations and the total number of expected calculations).
        :param verbose: if True, prints progress report to command line
        :return: the percentage of completed calculations for the current workflow directory
        """

        def format_percentage(total: int, percentage: float) -> str:
            """Formats total count and percentage into a string"""
            percentage_str = "({}%)".format(round(percentage * 100, 1))
            return "{0:<3} {1:>8}".format(total, percentage_str)

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
        num_molecules = len(glob(str(workflow_dir / "unopt_pdbs" / "*0.pdb")))
        num_structures = len(glob(str(workflow_dir / "unopt_pdbs" / "*.pdb")))
        total_num_completed = 0
        total_num_calcs = 0
        for step_id in config.get_step_ids():
            step_config = config.get_step(step_id)

            step_dir = workflow_dir / step_id
            completed_dir = step_dir / "completed"
            failed_dir = step_dir / "failed"
            output_file_ext = FlowRunner.PROGRAM_OUTFILE_EXTENSIONS[step_config["program"]]

            if step_config["conformers"]:
                num_jobs = num_structures
            else:
                num_jobs = num_molecules
            total_num_calcs += num_jobs

            num_completed = len(glob(str(completed_dir / "*.{}".format(output_file_ext))))
            completion_rate = num_completed / num_jobs
            total_num_completed += num_completed

            num_failed = len(glob(str(failed_dir / "*.{}".format(output_file_ext))))
            failure_rate = num_failed / num_jobs

            num_incomplete = num_jobs - num_completed
            incompletion_rate = num_incomplete / num_jobs

            running_jobs = []
            for f in glob(str(step_dir / "*.{}".format(output_file_ext))):
                mtime = datetime.fromtimestamp(os.path.getmtime(f))
                now = datetime.now()

                time_since_mtime = now - mtime
                if time_since_mtime.seconds < (5 * 60):
                    running_jobs.append(f)

            num_running = len(running_jobs)
            running_rate = num_running / num_jobs

            if verbose:
                result_entry = {"Step ID": step_id,
                                "Completed": format_percentage(num_completed, completion_rate),
                                "Incomplete": format_percentage(num_incomplete, incompletion_rate),
                                "Running": format_percentage(num_running, running_rate),
                                "Failed": format_percentage(num_failed, failure_rate)}

                results_table = results_table.append(result_entry, ignore_index=True, sort=False)

        total_completion_rate = round(100 * (total_num_completed / total_num_calcs), 1)

        if verbose:
            current_time_str = "[{}]".format(datetime.now().strftime("%b %d %Y %X"))
            print("\nProgress report for workflow '{}' {}".format(workflow_dir.name, current_time_str))
            print("Num. Molecules: {} ({})".format(num_molecules, num_structures))
            print(tabulate(results_table, headers="keys", tablefmt='psql', showindex=False))
            print("Overall completion rate: {}/{} ({}%)".format(total_num_completed, total_num_calcs,
                                                                total_completion_rate))

        return total_completion_rate

    @staticmethod
    def track_new_flow(config_file: Path, config_id: str, workflow_main_dir: Path) -> None:
        """
        Adds the specified workflow to the tracked_workflows.csv file. The workflow is
        added as a new row with columns for the config filepath, config_id, user,
        the run directory, the submission date and time, and the progress.
        :param config_file: a Path object pointing to the workflow config file for the current workflow
        :param config_id: the configuration ID for the current workflow
        :param workflow_main_dir: the main directory in where the workflow is running
        :return: None
        """

        # workflow tracking
        print("Tracking workflow...")
        workflow_id = workflow_main_dir.name
        flow_tracker = FlowTracker(workflow_id=workflow_id)

        new_flow_info = {"config_file": config_file.as_posix(),
                         "config_id": config_id,
                         "user": getuser(),
                         "run_directory": workflow_main_dir.as_posix(),
                         "submission_date": datetime.today().strftime("%d-%m-%Y"),
                         "submission_time": datetime.today().strftime("%H:%M:%S"),
                         "progress": "0%"}

        flow_tracker.track_flow(**new_flow_info)

    @staticmethod
    def view_tracked_flows(workflow_id: str = None, user: str = None, config_file: str = None) -> None:
        """
        Method for viewing a list of tracked workflows.
        :param workflow_id: the workflow ID to view
        :param user: the user to view
        :param config_file: the path to the config file to view
        :return: None
        """

        df = pd.read_csv(FlowTracker.TRACK_FILE, index_col=False)

        if workflow_id is not None:
            df = df.loc[df["workflow_id"] == workflow_id]
        if user is not None:
            df = df.loc[df["user"] == user]
        if config_file is not None:
            config_file = Path(config_file).resolve().as_posix()
            df = df.loc[df["config_file"] == config_file]

        print(tabulate(df, headers="keys", tablefmt='psql', showindex=False))
