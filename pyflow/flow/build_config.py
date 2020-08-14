import copy
import json
import sys
from pathlib import Path
from typing import Any

from pyflow.flow.flow_config import FlowConfig


def build_config(config_file: str, config_id: str, verbose: bool = False) -> None:
    """
    Method for helping the user build a flow configuration file for a custom
    workflow. Asks the user a series of command-line queries to construct the
    configuration file.

    :return: None
    """

    def request_step_id() -> str:
        """
        Asks the user to input a step ID through the command line.

        :return: the step ID input by the user
        """
        valid_step_id = False
        while not valid_step_id:
            step_id = str(input("Please specify a step ID: "))
            valid_step_id = FlowConfig.valid_step_id(step_id)
            if not valid_step_id:
                print("'{}' is an invalid step ID (step IDs should be at least "
                      "one character long and cannot start with an underscore)".format(step_id))
        return step_id

    def request_step_program(step_id: str) -> str:
        """
        Asks the user to input a step program through the command line.

        :param step_id: the step for which the program is being selected
        :return: the step program input by the user
        """
        valid_step_program = False
        while not valid_step_program:
            select_program_msg = "Please select the QC program for step '{}': ".format(step_id)
            step_program = str(input(select_program_msg)).lower()
            valid_step_program = FlowConfig.valid_step_program(step_program)
            if not valid_step_program:
                print("'{}' is an invalid QC program.".format(step_program))
        return step_program

    def convert_type(value: str, desired_type: Any) -> Any:
        """
        Converts the given string value to the desired type.

        :param value: some string value
        :param desired_type: the desired type
        :return: the value with its type converted
        """
        if desired_type == bool:
            if value.lower() == "true":
                return True
            elif value.lower() == "false":
                return False
            else:
                raise ValueError("Value '{}' cannot be cast to type <bool>".format(value))
        else:
            return desired_type(value)

    def add_step(step_id: str = None, step_program: str = None, config: dict = None) -> dict:
        """
        Adds a step to the the given config. If the given config is None,
        creates a new config with the specified step as the initial step.

        :param step_id: the new step ID to add
        :param step_program: the step program for the new step ID
        :param config: the config to add the step to
        :return:
        """
        if step_id is None:
            step_id = request_step_id()
        if step_program is None:
            step_program = request_step_program(step_id)

        if config is None:
            config = {"initial_step": step_id, "steps": {}}

        # create default dictionary of step parameters
        step_params = copy.deepcopy(FlowConfig.SUPPORTED_STEP_PARAMS["all"])
        for k, v in copy.deepcopy(FlowConfig.SUPPORTED_STEP_PARAMS[step_program]).items():
            step_params[k] = v
        step_params["program"] = step_program

        # request values for required step parameters
        for p in FlowConfig.REQUIRED_STEP_PARAMS[step_program]:
            value_type = type(step_params.pop(p))
            value = convert_type(input("Please specify a value for parameter '{}': ".format(p)), value_type)
            step_params[p] = value

        # customize step parameters
        print("Current parameters for step '{}':\n{}".format(step_id, json.dumps(step_params, indent=4)))
        custom_params_msg = "Please specify a comma-separated list of parameters " \
                            "and desired values for step '{}' (e.g., param1=val1, param2=val2)," \
                            " otherwise press Return to use default values:\n".format(step_id)
        new_vals = str(input(custom_params_msg))
        if len(new_vals.strip()) > 0:
            new_vals = [new_val.strip() for new_val in new_vals.split(",")]
            for val in new_vals:
                k, v = val.split("=")
                if k in step_params:
                    value_type = type(step_params[k])
                    step_params[k] = convert_type(v, value_type)

        config["steps"][step_id] = step_params

        # add dependent steps
        dependents = str(input("Please specify a comma-separated list of dependent step IDs for step '{}',"
                               " otherwise, press Return to finish building this step:\n".format(step_id)))

        if len(dependents.strip()) > 0:
            dependents = [dependent.strip() for dependent in dependents.split(",")]
            step_params["dependents"] = dependents
            for d in dependents:
                add_step(step_id=d, config=config)

        return config

    # check if config_file exists
    config_file = Path(config_file)
    if config_file.exists():
        if not FlowConfig.valid_config_file(config_file):
            raise IOError("'{}' already exists but is an invalid config_file".format(config_file))
        with config_file.open("r") as f:
            existing_config = json.load(f)
        if config_id in existing_config:
            print("The config_id '{}' already exists in config_file '{}'".format(config_id, config_file))
            sys.exit(1)
        else:
            print("Appending new workflow configuration with ID '{}' to config file '{}'".format(config_id,
                                                                                                 config_file))
    else:
        existing_config = {}

    print("Initial step configuration:")
    new_config = add_step()
    existing_config[config_id] = new_config

    if verbose:
        print(json.dumps(new_config, indent=4))

    with config_file.open("w") as f:
        f.write(json.dumps(existing_config, indent=4))
    print("Workflow configuration written to '{}'".format(config_file))
