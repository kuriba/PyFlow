import argparse
import sys
import textwrap

from pyflow.flow.flow_config import CONFIG_FILE
from pyflow.flow.flow_utils import get_path_to_pyflow


class FlowAction:
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
        parser = argparse.ArgumentParser(
            description="Parser for interacting with pyflow through the command line",
            formatter_class=argparse.RawTextHelpFormatter)

        parser.add_argument(
            'action',
            choices=('begin', 'run', 'handle', 'progress', 'setup', 'conformers'),
            type=str,
            help=FlowAction.ACTION_HELP)

        try:
            args = vars(parser.parse_args(sys.argv[1:2]))
        except:
            parser.print_help()
            sys.exit(0)

        if not hasattr(self, args["action"]):
            print('Unknown command: {}'.format(args["action"]))
            parser.print_help()
            sys.exit(1)

        getattr(self, args["action"])()

    def setup(self):
        from pyflow.flow.setup_flow import setup_dirs

        parser = argparse.ArgumentParser(description="PyFlow workflow directory setup")

        parser.add_argument(
            "-n", "--name",
            type=str,
            required=True,
            help="the name of the workflow")

        parser.add_argument(
            "-l", "--location",
            type=str,
            default=".",
            help="the location in which to create the workflow directory")

        parser.add_argument(
            "-f", "--config_file",
            type=str,
            default=get_path_to_pyflow() / "conf" / CONFIG_FILE,
            help="the path to the configuration file")

        parser.add_argument(
            "-id", "--config_id",
            type=str,
            default="default",
            help="the id of the desired workflow configuration")

        args = vars(parser.parse_args(sys.argv[2:]))

        setup_dirs(args)

    def begin(self):
        from pyflow.flow.begin_flow import main

        main()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Parser for interacting with pyflow through the command line",
        formatter_class=argparse.RawTextHelpFormatter)

    action_help = textwrap.dedent("""
        begin = begins a workflow
        run = runs a calculation
        handle = handles a completed, failed, or timed-out calculation
        progress = displays the progress for the current workflow
        setup = sets up a new workflow directory
        conformers = generate conformers
        """)

    parser.add_argument(
        'action',
        choices=('begin', 'run', 'handle', 'progress', 'setup', 'conformers'),
        type=str,
        help=action_help)

    parser.add_argument(
        '-s', '--step_id',
        type=str,
        required='run' in sys.argv or 'handle' in sys.argv,
        help="the step ID for which to take action")

    parser.add_argument(
        '-t', '--task_id',
        type=int,
        required='run' in sys.argv or 'handle' in sys.argv,
        help="the array task ID used to determine which molecule to run or handle")

    try:
        args = vars(parser.parse_args())
    except:
        parser.print_help()
        sys.exit(0)

    return args


def main():
    FlowAction()

    # TODO define main function as command line entry point
