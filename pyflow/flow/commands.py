import subprocess


class Commands:
    """

    """

    @classmethod
    def slurm_report(cls):
        subprocess.run([])

    @staticmethod
    def get_run_command(step_id: str, timelim: int):
        command = "pyflow run --step_id \"{}\" --task_id \"$SLURM_ARRAY_TASK_ID\" --time {}"
        return command.format(step_id, timelim)

    @staticmethod
    def get_handle_command(step_id: str):
        command = "pyflow handle --step_id \"{}\" --task_id \"$SLURM_ARRAY_TASK_ID\""
        return command.format(step_id)
