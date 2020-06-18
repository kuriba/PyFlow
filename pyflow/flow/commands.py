import subprocess


class Commands:
    """

    """

    @classmethod
    def slurm_report(cls):
        subprocess.run([])

    @staticmethod
    def get_run_command(step_id: str, timelim: int):
        command = "pyflow run --step_id \"{}\" --time {}"
        return command.format(step_id, timelim)

    @staticmethod
    def get_handle_command(step_id: str):
        command = "pyflow handle --step_id \"{}\""
        return command.format(step_id)
