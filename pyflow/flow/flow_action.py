import argparse
import sys
import textwrap

from pyflow.flow.flow_utils import get_default_config_file, get_path_to_pyflow


class FlowAction:
    """
    Class for parsing and delegating ``pyflow`` commands to relevant functions
    and classes. The ``pyflow`` command is specified as a ``console_scripts``
    entry point in ``setup.py``. Specifically, the ``pyflow`` command points to the
    :meth:`main<pyflow.flow.command_line.main>` function in :mod:`pyflow.flow.command_line`
    """

    ACTION_CHOICES = ('begin', 'run', 'handle', 'progress', 'setup', 'conformers',
                      'g16', 'sbatch', 'update')

    ACTION_HELP = textwrap.dedent("""
        Actions:
        begin = begin a workflow
        run = run a calculation
        handle = handle a completed, failed, or timed-out calculation
        progress = display the progress for the current workflow
        setup = set up a new workflow directory
        conformers = generate conformers
        g16 = write a Gaussian 16 input file
        sbatch = write a Slurm submission script
        update = download the latest Pyflow code from GitHub""")

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
            usage="pyflow <ACTION> <ARGUMENTS>",
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
            help="time limit in minutes")

        args = vars(parser.parse_args(sys.argv[2:]))

        FlowRunner.run_array_calc(args["step_id"], time=args["time"])

    def handle(self) -> None:
        """
        Handles processing of output from a quantum chemistry calculation run as
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

    def g16(self) -> None:
        """
        Method used to create Gaussian 16 input files.

        :return: None
        """
        from pyflow.io import gaussian_writer

        args = gaussian_writer.parse_args(sys.argv[2:])

        gaussian_writer.main(args)

    def sbatch(self) -> None:
        """
        Method used to create Slurm submission scripts.

        :return: None
        """
        from pyflow.io import sbatch_writer

        args = sbatch_writer.parse_args(sys.argv[2:])

        sbatch_writer.main(args)

    def update(self) -> None:
        """
        Method used to update Pyflow source code with most recent GitHub version.

        :return: None
        """
        import git
        git_dir = git.cmd.Git(get_path_to_pyflow())
        msg = git_dir.pull()
        print(msg)

    def build_config(self) -> None:
        """
        Method used to build a workflow configuration file.

        :return: None
        """
        from pyflow.flow.flow_config import FlowConfig

        parser = argparse.ArgumentParser(description="Build a workflow configuration file")

        parser.add_argument(
            "-c", "--config_file",
            type=str,
            required=True,
            help="the path to the file to write the workflow configuration to")

        parser.add_argument(
            "-i", "--config_id",
            type=str,
            required=False,
            default="default",
            help="the ID for the new workflow configuration")

        args = vars(parser.parse_args(sys.argv[2:]))

        FlowConfig.build_config(**args)


def main():
    FlowAction()
