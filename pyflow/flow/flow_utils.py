import json
import os
from glob import glob
from pathlib import Path

from pyflow.io.io_utils import upsearch

# files from which to load run parameters
CONFIG_FILE = "flow_config.json"
RUN_PARAMS_FILENAME = "run_params.json"
WORKFLOW_PARAMS_FILENAME = ".params"
LONG_TERM_STORAGE = "/work/lopez/workflows/"


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

    run_params_file = get_default_params_file()

    with run_params_file.open() as f:
        params = json.load(f)

    if program is None:
        return params[config_id]
    else:
        return params[config_id][program]


def load_workflow_params() -> dict:
    """
    Returns a dict of high level workflow details stored in the .params file in
    the main directory of a workflow. These details include the path to the workflow ]
    configuration file which details all of the steps, the configuration ID, and
    the number of conformers in the workflow.

    :return: a dict of workflow configuration details
    :raises FileNotFoundError: if .params file is not found
    """

    try:
        workflow_params_file = upsearch(WORKFLOW_PARAMS_FILENAME)
    except FileNotFoundError:
        message = "Unable to find .params file; ensure that you are in a workflow directory."
        raise FileNotFoundError(message)

    with workflow_params_file.open() as f:
        workflow_params = json.load(f)

    return workflow_params


def update_workflow_params(**kwargs) -> None:
    """
    Updates the specified workflow parameters in the .params file with the
    specified values.
    :param kwargs: a dict of parameters and new values
    :return: None
    """
    workflow_params_file = upsearch(WORKFLOW_PARAMS_FILENAME)
    workflow_params = load_workflow_params()
    for k, v in kwargs.items():
        if k in workflow_params:
            workflow_params[k] = v

    with workflow_params_file.open("w") as f:
        f.write(json.dumps(workflow_params, indent=4))


def get_path_to_pyflow() -> Path:
    """
    Returns a ``Path`` object which points to the ``PYFLOW`` environment variable.
    ``PYFLOW`` should point to the directory containing the pyflow module.
    :return: a Path object with the path to ``PYFLOW``
    """
    return Path(os.environ["PYFLOW"])


def get_default_config_file() -> Path:
    """
    Returns a ``Path`` object pointing to the default config file.
    :return: a Path object
    """
    return get_path_to_pyflow() / "pyflow" / "conf" / CONFIG_FILE


def get_default_params_file():
    """
    Returns a ``Path`` object pointing
    :return:
    """
    return get_path_to_pyflow() / "pyflow" / "conf" / RUN_PARAMS_FILENAME


def get_num_conformers(inchi_key: str) -> int:
    """
    Returns the number of conformers for the molecule with the given InChIKey.

    :param inchi_key: the InChIKey of the molecule
    :return: the number of conformers
    """
    params_file = upsearch(WORKFLOW_PARAMS_FILENAME)

    unopt_pdbs = params_file.parent / "unopt_pdbs" / "{}*.pdb".format(inchi_key)

    num_conformers = len(glob(str(unopt_pdbs)))

    return num_conformers


def copy_to_long_term_storage() -> None:
    pass  # TODO implement copy_to_long_term_storage
