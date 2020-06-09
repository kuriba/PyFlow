import json
import os
from pathlib import Path

# files from which to load run parameters
from pyflow.io.io_utils import upsearch

RUN_PARAMS_FILENAME = "run_params.json"
WORKFLOW_PARAMS_FILENAME = ".params"


def load_run_params(config_id: str = "default", program: str = None) -> dict:
    """
    Loads the workflow configuration information stored in the ``run_params.json``
    file of a workflow into a dictionary.

    The general structure of the dictionary is as follows:
    ::
        { "default":
            {
              "slurm": {...},
              "gaussian16": {...},
              "gamess": {...},
              ...
            },
            ...
        }

    There is a general dictionary of "default" parameters which contains
    sub-dictionaries that store run parameters relevant to each program. It
    is possible to define new sets of run parameters by simply adding new
    dicts below the "default" dict (each dict should also have a unique
    string ID).

    The exact parameters stored for each program varies. For instance,
    the ``"slurm"`` block stores a ``"PARTITION"`` parameter which is not
    relevant to Gaussian 16 or GAMESS.

    :param config_id: unique string ID for the desired run parameters
    :param program: program for which to load parameters
    :return: a dictionary with workflow run parameters
    """

    params_file = get_path_to_pyflow() / "conf" / RUN_PARAMS_FILENAME

    with params_file.open() as f:
        params = json.load(f)

    if program is None:
        return params[config_id]
    else:
        return params[config_id][program]


def load_workflow_params() -> dict:
    """
    Returns a dict of high level workflow  details stored in the .params file in
    the main directory of a workflow. These details include the path to the workflow ]
    configuration file which details all of the steps, the configuration ID, and
    the number of conformers in the workflow.

    :return: a dict of workflow configuration details
    :raises FileNotFoundError: if .params file is not found
    """

    try:
        workflow_config_file = upsearch(WORKFLOW_PARAMS_FILENAME)
    except FileNotFoundError:
        message = "Unable to find .params file; ensure that you are in a workflow directory."
        raise FileNotFoundError(message)

    with workflow_config_file.open() as f:
        workflow_config = json.load(f)

    return workflow_config


def get_path_to_pyflow() -> Path:
    """
    Returns a ``Path`` object which points to the ``PYFLOW`` environment variable.
    :return: a Path object with the path to ``PYFLOW``
    """
    return Path(os.environ["PYFLOW"])


def get_num_conformers() -> int:
    """
    Returns the number of conformers for each molecule in the current workflow.
    :return: the number of conformers
    """
    params_file = upsearch(WORKFLOW_PARAMS_FILENAME)

    with params_file.open() as f:
        config = json.load(f)
    try:
        return config["num_conformers"]
    except KeyError:
        message = "'num_conformers' not yet defined in {}".format(WORKFLOW_PARAMS_FILENAME)
        raise KeyError(message)
