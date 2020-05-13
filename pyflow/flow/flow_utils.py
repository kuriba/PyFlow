import json
from pyflow.io.io_utils import upsearch
from pyflow.flow.flow_config import CONFIG_FILE

# files from which to load run parameters
PARAMS_FILE = ".run_params"


def load_run_params(config_id: str = "default", program: str = None) -> dict:
    """
    Loads the workflow configuration information stored in the ``.run_params``
    file of a workflow into a dictionary. The file is assumed to be located
    in the root directory of the workflow.

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
    params_file = upsearch(PARAMS_FILE)
    with open(params_file) as f:
        params = json.load(f)
    if program is None:
        return params[config_id]
    else:
        return params[config_id][program]


def load_flow_config(config_id: str = "default", step_id: str = None) -> dict:
    """
    Loads the workflow configuration stored in the ``.flow_config`` file of
    a workflow into a dictionary. The ``.flow_config`` file uses the JSON
    format to store the calculation steps for a workflow.

    There is a general dictionary with the ID "default" which corresponds to
    the default VERDE Materials DB workflow.  #TODO add workflow image link
    The dictionary stores an ``"initialStep"`` parameter to determine where to
    start a workflow. The steps of the workflow are stored in a nested
    dictionary under the key ``"steps"``. Each step may define various parameters
    including the following:

    + ``"program"``
    + ``"opt"``
    + ``"route"`` (if ``"program" == "gaussian16"``)
    + ``"dependents"``
    + ``"conformers"``

    For more details on the ``.flow_config`` file and accepted parameters
    see :class:`pyflow.flow.flow_config.FlowConfig`.


    :param config_id: unique string ID for the desired workflow configuration
    :param step_id: the step to load
    :return: a dictionary with a workflow configuration
    """

    config_file = upsearch(CONFIG_FILE)
    with open(config_file) as f:
        config = json.load(f)
    if step_id is None:
        return config[config_id]
    else:
        return config[config_id]["steps"][step_id]
