from pyflow.io.io_utils import upsearch

CONFIG_FILE = ".flow_config"

class FlowConfig:
    """
    Class for parsing and storing the configuration file of a workflow. The
    configuration file is named ``.flow_config`` and is found at the root of a
    workflow directory. It uses the JSON format to store the calculation steps
    for a workflow.

    The general structure of the dictionary is as follows:
    ::
        { "default":
            {
                "initialStep": "X",
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

    #TODO define and list flow_config parameters
    """

    def __init__(self):

        #TODO write FlowConfig
        pass

