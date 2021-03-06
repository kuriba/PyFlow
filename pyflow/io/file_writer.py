from __future__ import annotations

import sys
from pathlib import Path

from pyflow.io.io_utils import yes_no_query
from pyflow.mol import mol_utils
from pyflow.mol.mol_utils import get_formatted_geometry, get_supported_babel_formats


class FileWriter:
    """
    Generalized class for writing text to a file.
    """

    def __init__(self, filepath: Path, text: str = "", overwrite: bool = False):
        """
        Constructs a FileWriter object to write a text file at the specified
        ``filepath``.

        :param filepath: the path to the write file
        :param text: the text to write
        :param overwrite: if True, will overwrite specified file without prompting user
        """
        self.filepath = Path(filepath).resolve()
        self.text = text
        self.overwrite = overwrite

    def write(self):
        """
        Writes ``self.text`` to the file located at ``self.filepath``.

        :return: None
        """
        if self.overwrite or not self.filepath.exists():
            self.filepath.write_text(self.text)
        else:
            message = "{} already exists. Do you wish to overwrite this file?" \
                      "[y/n]:\n"

            overwrite = yes_no_query(message.format(self.filepath.as_posix()))
            if overwrite:
                self.filepath.write_text(self.text)

    def append(self, text):
        """
        Appends the given text to this FileWriter's text block.

        :param text: the text to append
        :return: None
        """
        self.text += text

    def get_text(self):
        """
        Gets the currently stored text.

        :return: self.text
        """
        return self.text


class AbstractInputFileWriter(FileWriter):
    """
    Abstract class for writing input files.
    """

    def __init__(self,
                 geometry_file: Path,
                 geometry_format: str,
                 filepath: Path = None,
                 overwrite: bool = False,
                 **kwargs):
        """
        Constructor for an AbstractInputFileWriter. This class abstracts the most
        general fields and methods of input file writers.

        :param program: quantum chemistry program
        :param filepath: path to the new input file
        :param geometry_file: initial coordinates for the input file
        :param geometry_format: format of the initial coordinates file
        :param overwrite: if True, will overwrite specified file without prompting user
        :param kwargs: keyword arguments specific to each type of input file
        """
        if filepath is None:
            filepath = Path().cwd() / geometry_file.name

        super().__init__(filepath=filepath, overwrite=overwrite)

        self.args = {"title": filepath.stem,
                     "location": filepath.parent,
                     "geometry_format": geometry_format,
                     "geometry_file": Path(geometry_file).resolve()}

        if kwargs.get("formats", False):
            for i in get_supported_babel_formats():
                print(i)
            sys.exit()

        for k, v in kwargs.items():
            if k not in self.args:
                self.args[k] = v

        # get formatted coordinates
        self.coordinates = get_formatted_geometry(str(self.args["geometry_file"]),
                                                  geometry_format=geometry_format,
                                                  output_format=self.get_openbabel_format())

        # charge
        if self.args.get("smiles_geometry_file") is None:
            self.args["smiles_geometry_file"] = self.args["geometry_file"]
            self.args["smiles_geometry_format"] = self.args["geometry_format"]

        smiles = mol_utils.get_smiles(str(self.args["smiles_geometry_file"]),
                                      geometry_format=self.args["smiles_geometry_format"])

        self.args["charge"] = mol_utils.get_charge(smiles) + self.args.get("charge", 0)

    @classmethod
    def from_config(cls,
                    step_config: dict,
                    filepath: Path,
                    geometry_file: Path,
                    geometry_format: str,
                    **kwargs) -> AbstractInputFileWriter:
        """
        Constructs an AbstractInputFileWriter based on the configuration of a
        workflow step.

        :param step_config: the workflow step configuration
        :param filepath: path to the new input file
        :param geometry_file: initial coordinates for the input file
        :param geometry_format: format of the initial coordinates file
        :param kwargs: keyword arguments specific to each type of input file
        :return: a new AbstractInputFileWriter object
        """
        return cls(filepath=filepath,
                   geometry_file=geometry_file,
                   geometry_format=geometry_format,
                   **step_config,
                   **kwargs)

    @classmethod
    def get_openbabel_format(self) -> str:
        raise NotImplementedError
