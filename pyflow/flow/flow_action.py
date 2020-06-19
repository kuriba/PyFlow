import argparse
import sys
import textwrap

from pyflow.flow.flow_utils import get_default_config_file


class FlowAction:
    """
    Class for  parsing and delegating ``pyflow`` commands to relevant functions
    and classes. The ``pyflow`` command is specified as a ``console_scripts``
    entry point in ``setup.py``. Specifically, the ``pyflow`` command points to the
    :meth:`main<pyflow.flow.command_line.main>` function in :mod:`pyflow.flow.command_line`
    """

    ACTION_CHOICES = ('begin', 'run', 'handle', 'progress', 'setup', 'conformers')

    ACTION_HELP = textwrap.dedent("""
        Subcommand to run:
        begin = begins a workflow
        run = runs a calculation
        handle = handles a completed, failed, or timed-out calculation
        progress = displays the progress for the current workflow
        setup = sets up a new workflow directory
        conformers = generate conformers
        """)

    def __init__(self):
        """
        Constructs a FlowAction object which allows the user to select an action
        from the ``ACTION_CHOICES`` class variable. If a method with the same name
        as the selected action exists, the parsing of the remaining command line
        arguments is delegated to the relevant method.

        For example, to begin a workflow from the command line, you would use
        ``pyflow begin``, followed by any options or flags that the ``begin``
        method in FlowAction supports.

        To add a new action, you must add it to the ``ACTION_CHOICES`` class variable,
        then write a new method of the same name within this class.
        """
        parser = argparse.ArgumentParser(
            description="Parser for interacting with pyflow through the command line",
            formatter_class=argparse.RawTextHelpFormatter)

        parser.add_argument(
            'action',
            choices=FlowAction.ACTION_CHOICES,
            type=str,
            help=FlowAction.ACTION_HELP)

        try:
            args = parser.parse_args(sys.argv[1:2])
        except:
            # parser.print_help()
            sys.exit(0)

        if not hasattr(self, args.action):
            print('Unknown command: {}'.format(args.action))
            parser.print_help()
            sys.exit(1)

        getattr(self, args.action)()

    def setup(self) -> None:
        """
        Method used to set up workflow directories from the command line.

        :return: None
        """
        from pyflow.flow.setup_flow import setup_dirs

        parser = argparse.ArgumentParser(description="PyFlow workflow directory setup")

        parser.add_argument(
            "-n", "--name",
            type=str,
            required=True,
            dest="workflow_name",
            help="the name of the workflow")

        parser.add_argument(
            "-l", "--location",
            type=str,
            default=".",
            dest="save_location",
            help="the location in which to create the workflow directory")

        parser.add_argument(
            "-f", "--config_file",
            type=str,
            default=get_default_config_file(),
            help="the path to the configuration file")

        parser.add_argument(
            "-id", "--config_id",
            type=str,
            default="default",
            help="the id of the desired workflow configuration")

        args = vars(parser.parse_args(sys.argv[2:]))

        setup_dirs(**args)

    def begin(self) -> None:
        """
        Begins running a workflow.

        :return:
        """
        from pyflow.flow.begin_step import begin_step

        parser = argparse.ArgumentParser(description="Begin running a workflow step")

        parser.add_argument(
            "-s", "--step_id",
            type=str,
            required=False,
            help="the step ID to run")

        args = vars(parser.parse_args(sys.argv[2:]))

        begin_step(step_id=args.get("step_id"))

    def run(self) -> None:
        """
        Runs a quantum chemistry calculation as part of a Slurm array.

        :return:
        """
        from pyflow.flow.flow_runner import FlowRunner

        parser = argparse.ArgumentParser(description="Run quantum chemistry calculation")

        parser.add_argument(
            "-s", "--step_id",
            type=str,
            required=True,
            help="the step ID to run")

        parser.add_argument(
            "-t", "--time",
            type=int,
            required=True,
            dest="timelimit",
            help="time limit in minutes")

        args = vars(parser.parse_args(sys.argv[2:]))

        FlowRunner.run_array_calc(args["step_id"], timelimit=args["timelimit"])

    def handle(self) -> None:
        """
        Handle processing of output from a quantum chemistry calculation run as
        part of an array

        :return: None
        """
        from pyflow.flow.flow_runner import FlowRunner

        parser = argparse.ArgumentParser(description="Run quantum chemistry calculation")

        parser.add_argument(
            "-s", "--step_id",
            type=str,
            required=True,
            help="the step ID to run")

        args = vars(parser.parse_args(sys.argv[2:]))

        FlowRunner.handle_array_output(args["step_id"])


def main():
    FlowAction()
