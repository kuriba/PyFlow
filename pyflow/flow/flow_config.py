import json
from json import load, dumps
from pathlib import Path
from typing import List

from pyflow.flow.flow_utils import load_run_params

RUN_PARAMS = load_run_params()


class FlowConfig:
    """
    Class for parsing and storing the configuration file of a workflow. It uses
    the JSON format to store the calculation steps for a workflow.

    The general structure of the dictionary is as follows:
    ::
        { "default":
            {
                "initial_step": "X",
                "steps": {
                    "X": {
                        "program": "gaussian16",
                        "route": "#p pm7 opt",
                        "opt": true,
                        "conformers": true,
                        "dependents": ["Y"]
                    },
                    "Y": {...},
                    "Z": {...}
                }
            }
        }

    The "default" key is the config_id that refers to a flow configuration. The
    flow configuration defines two keys: ``initial_step`` and ``steps``.
    The former declares the first step of the workflow, and the latter defines the
    specific instructions for running each step. Each step must define a dictionary
    of step parameters including information such as partition, memory, and
    time limits (an exhaustive list of step parameters can be found below).

    Below is a list of general step parameters that are supported by all QC programs:

    +----------------------------+----------------------------------------------------+------------------+
    | Parameter                  | Description                                        | Data type        |
    +============================+====================================================+==================+
    | ``program``                | the QC program to use                              | ``str``          |
    +----------------------------+----------------------------------------------------+------------------+
    | ``opt``                    | whether the step includes an optimization          | ``bool``         |
    +----------------------------+----------------------------------------------------+------------------+
    | ``freq``                   | whether the step includes a frequency calculation  | ``bool``         |
    +----------------------------+----------------------------------------------------+------------------+
    | ``single_point``           | whether the step is a single-point calculation     | ``bool``         |
    +----------------------------+----------------------------------------------------+------------------+
    | ``conformers``             | whether the step has conformers                    | ``bool``         |
    +----------------------------+----------------------------------------------------+------------------+
    | ``proceed_on_failed_conf`` | if True, allow molecules with failed conformers    | ``bool``         |
    |                            | to proceed to the next step                        |                  |
    +----------------------------+----------------------------------------------------+------------------+
    | ``attempt_restart``        | whether to attempt to restart a calculation upon   | ``bool``         |
    |                            | timeout or failure                                 |                  |
    +----------------------------+----------------------------------------------------+------------------+
    | ``nproc``                  | number of cores to request through Slurm           | ``int``          |
    +----------------------------+----------------------------------------------------+------------------+
    | ``memory``                 | amount of memory to request in GB                  | ``int``          |
    +----------------------------+----------------------------------------------------+------------------+
    | ``time``                   | the time limit for the calculation in minutes      | ``int``          |
    +----------------------------+----------------------------------------------------+------------------+
    | ``time_padding``           | the time limit for processing/handling calculation | ``int``          |
    |                            | outputs (the overall time limit for the Slurm      |                  |
    |                            | submission is ``time + time_padding``)             |                  |
    +----------------------------+----------------------------------------------------+------------------+
    | ``partition``              | the partition to request for the step              | ``str``          |
    +----------------------------+----------------------------------------------------+------------------+
    | ``simul_jobs``             | the number of jobs to simultaneously run           | ``int``          |
    +----------------------------+----------------------------------------------------+------------------+
    | ``save_outputs``           | whether to save the results of a step in           | ``bool``         |
    |                            | /work/lopez/workflows                              |                  |
    +----------------------------+----------------------------------------------------+------------------+
    | ``dependents``             | a list of step IDs that are to be run after the    | ``List[string]`` |
    |                            | completion of the current step                     |                  |
    +----------------------------+----------------------------------------------------+------------------+
    | ``charge``                 | the charge by which to increment all molecules     | ``int``          |
    +----------------------------+----------------------------------------------------+------------------+
    | ``multiplicity``           | the multiplicity of the molecules                  | ``int``          |
    +----------------------------+----------------------------------------------------+------------------+

    Supported step parameters specific to certain QC programs are shown below
    (refer to the documentation specific to each QC program for more details on
    valid arguments for each parameter):

    +-------------+----------------------------+---------------------------------------------------+------------------+
    | QC program  | Parameter                  | Description                                       | Data type        |
    +=============+============================+===================================================+==================+
    | gaussian16  | ``route`` *                | the full route for the calculation                | ``str``          |
    |             +----------------------------+---------------------------------------------------+------------------+
    |             | ``rwf``                    | whether to save the .rwf file                     | ``bool``         |
    |             +----------------------------+---------------------------------------------------+------------------+
    |             | ``chk``                    | whether to save the .chk file                     | ``bool``         |
    +-------------+----------------------------+---------------------------------------------------+------------------+
    | gamess      | ``gbasis`` *               | Gaussian basis set specification                  | ``str``          |
    |             +----------------------------+---------------------------------------------------+------------------+
    |             | ``runtyp``                 | the type of computation                           | ``str``          |
    |             |                            | (e.g., energy, gradient, etc.)                    |                  |
    |             +----------------------------+---------------------------------------------------+------------------+
    |             | ``dfttyp``                 | DFT functional to use (ab initio if unspecified)  | ``str``          |
    |             +----------------------------+---------------------------------------------------+------------------+
    |             | ``maxit``                  | maximum number of SCF iteration cycles            | ``int``          |
    |             +----------------------------+---------------------------------------------------+------------------+
    |             | ``opttol``                 | gradient convergence tolerance, in Hartree/Bohr   | ``float``        |
    |             +----------------------------+---------------------------------------------------+------------------+
    |             | ``hess``                   | selects the initial Hessian matrix                | ``str``          |
    |             +----------------------------+---------------------------------------------------+------------------+
    |             | ``nstep``                  | maximum number of steps to take                   | ``int``          |
    |             +----------------------------+---------------------------------------------------+------------------+
    |             | ``idcver``                 | the dispersion correction implementation to use   | ``int``          |
    +-------------+----------------------------+---------------------------------------------------+------------------+
    """

    # list of supported programs
    SUPPORTED_PROGRAMS = ["gaussian16", "gamess"]

    # dict of parameters required in the config file
    REQUIRED_GENERAL_PARAMS = ["initial_step", "steps"]

    # dict of required step parameters for each type of program
    REQUIRED_STEP_PARAMS = {"all": ["program"],
                            "gaussian16": ["route"],
                            "gamess": ["gbasis"]}

    # dict of required directories for step parameters
    REQUIRED_STEP_DIRS = {"all": {"opt": ["completed", "failed"],
                                  "single_point": ["completed", "failed"]},
                          "gaussian16": {"freq": ["completed", "failed"]}}

    # dict of supported general configuration parameters and their expected types
    SUPPORTED_GENERAL_PARAMS = {"initial_step": str,
                                "steps": dict}

    # dict of all supported step parameters and their default values for each workflow step
    SUPPORTED_STEP_PARAMS = {"all": {"opt": True,
                                     "single_point": False,
                                     "conformers": False,
                                     "proceed_on_failed_conf": True,
                                     "dependents": [],
                                     "charge": 0,
                                     "multiplicity": 1,
                                     "save_output": False,
                                     "partition": "short",
                                     "time_padding": RUN_PARAMS["slurm"]["time_padding"],
                                     "simul_jobs": 50},
                             "gaussian16": {"route": "#p",
                                            "freq": False,
                                            "attempt_restart": False,
                                            "nproc": RUN_PARAMS["gaussian16"]["nproc"],
                                            "memory": RUN_PARAMS["gaussian16"]["memory"],
                                            "time": RUN_PARAMS["gaussian16"]["time"],
                                            "rwf": False,
                                            "chk": False},
                             "gamess": {"attempt_restart": False,
                                        "memory": RUN_PARAMS["gamess"]["memory"],
                                        "nproc": RUN_PARAMS["gamess"]["nproc"],
                                        "time": RUN_PARAMS["gamess"]["time"],
                                        "runtyp": "OPTIMIZE",
                                        "dfttyp": "NONE",
                                        "maxit": 200,
                                        "gbasis": "",
                                        "opttol": 0.0005,
                                        "hess": "CALC",
                                        "nstep": 400,
                                        "idcver": 3}}

    def __init__(self, config_file: str, config_id: str):
        """
        Constructs a FlowConfig object which stores the configuration of the
        current workflow. The FlowConfig object validates the contents of the
        config file and makes it simple to determine the next steps.

        :param config_file: the config file to load
        :param config_id: the config ID to load
        """
        config = self.load_flow_config(config_file, config_id)
        self.config = self._add_missing_step_params(config)

    @staticmethod
    def load_flow_config(config_file: str, config_id: str) -> dict:
        """
        Loads the workflow configuration with the specified ``config_id`` stored
        in the file ``config_file``.

        For more details on flow configuration files and their accepted parameters
        see :class:`pyflow.flow.flow_config.FlowConfig`.

        :param config_file: path to the config file
        :param config_id: unique string ID for the desired workflow configuration
        :return: a dictionary with a workflow configuration
        """
        with open(config_file) as f:
            config = load(f)
        config = config[config_id]

        if FlowConfig.valid_config(config):
            return config
        else:
            message = "The file {} is not a valid workflow config file.".format(config_file)
            raise Exception(message)

    @staticmethod
    def valid_config(config: dict) -> bool:
        """
        Determines if the given config is validly formatted and contains
        parameters required by each program.

        :param config: a dictionary representing a workflow configuration
        :return: True if the workflow configuration is valid, False otherwise
        """
        # general validation
        for param in FlowConfig.REQUIRED_GENERAL_PARAMS:
            if param not in config:
                return False

        # validate types
        for param, expect_type in FlowConfig.SUPPORTED_GENERAL_PARAMS.items():
            if param in config:
                if not isinstance(config[param], expect_type):
                    print("Config error: invalid type given for parameter '{}'; expected type {}".format(config[param],
                                                                                                         expect_type))
                    return False

        # ensure consistency of initial_step with workflow steps
        if config["initial_step"] not in config["steps"]:
            print("Config error: initial step '{}' is not defined in steps".format(config["initial_step"]))
            return False

        # validation of each workflow step
        for step_id, step_config in config["steps"].items():

            # validate the step ID
            if not FlowConfig.valid_step_id(step_id):
                print("Config error: invalid step ID '{}'".format(step_id))
                return False

            # ensure that parameters required by all programs are defined
            for param in FlowConfig.REQUIRED_STEP_PARAMS["all"]:
                if param not in step_config:
                    print("Config error: missing parameter '{}' for step '{}'".format(param, step_id))
                    return False

            # ensure that the program is valid
            program = step_config["program"]
            if program not in FlowConfig.SUPPORTED_PROGRAMS:
                print("Config error: unsupported step program '{}' for step '{}'".format(program, step_id))
                return False

            # ensure that program-specific requirements are met
            if program in FlowConfig.REQUIRED_STEP_PARAMS:
                for param in FlowConfig.REQUIRED_STEP_PARAMS[program]:
                    if param not in step_config:
                        print("Config error: missing parameter '{}' for step '{}'".format(param, step_id))
                        return False

            # check that the specified parameters have valid types
            for prog in [program, "all"]:
                for param, default_val in FlowConfig.SUPPORTED_STEP_PARAMS[prog].items():
                    expect_type = type(default_val)
                    if param in step_config:
                        if not isinstance(step_config[param], expect_type):
                            print("Config error: invalid type for parameter '{}' in step '{}'".format(param, step_id))
                            return False

            return True

    @staticmethod
    def valid_config_file(config_file: Path) -> bool:
        """
        Determines if the given ``config_file`` is valid.

        :param config_file: the Path to the config file
        :return: True if valid, False otherwise
        """
        with config_file.open() as f:
            configs = json.load(f)
        return all([FlowConfig.valid_config(v) for k, v in configs.items()])

    @staticmethod
    def valid_step_id(step_id: str) -> bool:
        """
        Determines if the given ``step_id`` is valid. The length of the step ID
        should be greater than 0, and it should not include any underscores.

        :param step_id:
        :return:
        """
        return len(step_id) > 0 and "_" not in step_id

    @staticmethod
    def valid_step_program(step_program: str) -> bool:
        """
        Determines if the given ``step_program`` is valid.

        :param step_program: the name of the program
        :return: True if the program is valid, False otherwise
        """
        return step_program in FlowConfig.SUPPORTED_PROGRAMS

    def _add_missing_step_params(self, config: dict) -> dict:
        """
        Adds missing step parameters with values determined by defaults in
        ``SUPPORTED_STEP_PARAMS`` class variable defined in ``FlowConfig``
        (see :class:`pyflow.flow.flow_config.FlowConfig` for more details).

        :param config: the workflow configuration
        :return: a dict with missing step parameters added
        """
        updated_config = config.copy()

        for step_id, step_config in updated_config["steps"].items():
            step_program = step_config["program"]
            for program in ["all", step_program]:
                if program in FlowConfig.SUPPORTED_STEP_PARAMS:
                    for param, default_val in FlowConfig.SUPPORTED_STEP_PARAMS[program].items():
                        if param not in step_config:
                            step_config[param] = default_val

        return updated_config

    def get_step(self, step_id: str) -> dict:
        """
        Returns the parameters for the workflow step with the given ``step_id``.

        :param step_id: unique string ID for a workflow step
        :return: a dict of parameters for a workflow step
        """
        return self.config["steps"][step_id]

    def get_all_steps(self) -> dict:
        """
        Returns the parameters for all workflow steps.

        :return: a dict of workflow steps
        """
        return self.config["steps"]

    def get_step_ids(self) -> List[str]:
        """
        Returns an ordered list of step IDs for the current workflow.

        :return: a list of step IDs
        """
        initial_step = self.get_initial_step_id()
        step_ids = [initial_step]

        current_ids = [initial_step]
        while len(current_ids) > 0:
            new_ids = []
            for current_id in current_ids:
                dependents = self.get_dependents(current_id)
                step_ids.extend(dependents)
                new_ids.extend(dependents)

            current_ids = new_ids

        return step_ids

    def get_initial_step_id(self) -> str:
        """
        Returns the ID of the first step in the workflow.

        :return: the ID of the first workflow step
        """
        return self.config["initial_step"]

    def get_previous_step_id(self, step_id: str) -> str:
        """
        Returns the step ID of the step preceding the given step ID.

        :param step_id: the step ID whose previous step is desired
        :return: the previous step ID
        :raises ValueError: if the given step ID is the initial step or if it does not exist
        """
        if self.get_initial_step_id() == step_id:
            message = "{} is the initial step and therefore has no previous step".format(step_id)
            raise ValueError(message)

        for step in self.get_all_steps():
            if step_id in self.get_dependents(step):
                return step

        message = "No previous step found for the given step ID '{}'".format(step_id)
        raise ValueError(message)

    def get_dependents(self, step_id) -> List[str]:
        """
        Returns a list of ``step_ids`` which depend on (*i.e.*, require the
        completion of) the given ``step_id``.

        :param step_id: the unique string ID for a workflow step
        :return: a list of dependent workflow steps
        """
        dependents = self.get_step(step_id)["dependents"]
        return dependents

    def get_step_directories(self, step_id) -> List[str]:
        """
        Returns a list of directories needed to run the workflow step with the
        given ``step_id``.

        :param step_id: the unique string ID of the workflow step
        :return: a list of directories
        """
        step_config = self.get_step(step_id)
        step_program = step_config["program"]

        directories = []

        for program in ["all", step_program]:
            if program in FlowConfig.REQUIRED_STEP_DIRS:
                for param, required_dirs in FlowConfig.REQUIRED_STEP_DIRS[program].items():
                    if step_config[param]:
                        directories.extend(required_dirs)

        directories = list(set(directories))

        return directories

    def print(self):
        """
        Prints ``self.config`` to ``sys.stdout``.

        :return: None
        """
        print(dumps(self.config, indent=4))
