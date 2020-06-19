import subprocess


class Commands:
    """

    """

    @classmethod
    def slurm_report(cls):
        subprocess.run([])

    @staticmethod
    def get_run_command(step_id: str, timelim: int) -> str:
        """
        Command used for running an array calculation for the specified step ID.
        :param step_id:
        :param timelim:
        :return:
        """
        command = "pyflow run --step_id \"{}\" --time {}"
        return command.format(step_id, timelim)

    @staticmethod
    def get_handle_command(step_id: str) -> str:
        """
        Command used for handling a completed array calculation.
        :param step_id:
        :return:
        """
        command = "pyflow handle --step_id \"{}\""
        return command.format(step_id)

    @staticmethod
    def get_begin_step_command(step_id: str) -> str:
        """
        Command used to begin a workflow step.
        :param step_id:
        :return:
        """
        command = "pyflow begin --step_id \"{}\""
        return command.format(step_id)
