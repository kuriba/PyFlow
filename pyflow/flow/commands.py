class Commands:
    """
    Class with various static methods for constructing commands used to perform
    workflow actions such as beginning workflow steps, running calculations,
    and handling their outputs.
    """

    @staticmethod
    def get_run_command(step_id: str, wave_id: int, time: int) -> str:
        """
        Command used for running an array calculation for the specified step ID.
        :param step_id: the step ID to run
        :param wave_id: the wave ID to run
        :param time: the time limit for the calculation, in minutes
        :return: a string with the command to run a calculation
        """
        command = "pyflow run --wave_id {} --step_id \"{}\" --time {}"
        return command.format(wave_id, step_id, time)

    @staticmethod
    def get_handle_command(step_id: str, wave_id: int) -> str:
        """
        Command used for handling a completed array calculation.
        :param step_id: the step ID to handle
        :param wave_id: the wave ID to handle
        :return: a string with the command for handling the output of a calculation
        """
        command = "pyflow handle --wave_id {} --step_id \"{}\""
        return command.format(wave_id, step_id)

    @staticmethod
    def get_begin_step_command(step_id: str, wave_id: int, attempt_restart: bool = False) -> str:
        """
        Command used to begin a workflow step.
        :param step_id: the step ID to begin
        :param wave_id: the wave ID to begin or restart
        :param attempt_restart: if True, the given wave ID will be restarted
        :return:
        """
        if not attempt_restart:
            command = "pyflow begin --wave_id {} --step_id \"{}\""
        else:
            command = "pyflow begin --wave_id {} --step_id \"{}\" --attempt_restart"
        return command.format(wave_id, step_id)
