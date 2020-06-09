from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


class FlowTracker:
    """
    Class used for tracking submitted, running, and completed workflows.
    """

    # TODO update file path on Discovery
    TRACK_FILE = Path("C:\\Users\\abreha.b\\.PyCharm2019.3\\config\\scratches\\scratch.csv")

    REQUIRED_ATTRIBUTES = ["config_file", "config_id", "user", "run_directory",
                           "submission_date", "submission_time", "progress"]

    def __init__(self, workflow_id: str):
        """
        Constructs a workflow tracker.

        :param workflow_id: the name of the workflow
        """
        self.workflow_id = workflow_id

    def update(self, **kwargs):
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

    # TODO track workflow percentage progress
    # TODO mark unchaged workflows
