import os
from openbabel import openbabel
from pathlib import Path
from typing import List

from rdkit import Chem

from pyflow.io.io_utils import find_string


def get_charge(smiles: str) -> int:
    """
    Returns the charge of the given molecule based on its SMILES string.

    :param smiles: the SMILES string of the molecule
    :return: the charge of the molecule
    """
    mol = Chem.MolFromSmiles(smiles, sanitize=False)
    return Chem.GetFormalCharge(mol)


def get_formatted_geometry(geometry_file: str, output_format: str, geometry_format: str = None) -> str:
    """
    Returns the formatted molecular geometry from the given geometry file. The
    format of the geometry file is assumed based on the filename extension but
    can be specified with the ``geometry_format`` keyword argument.

    For a list of supported geometry formats, refer to the `Open Babel documentation
    <https://open-babel.readthedocs.io/en/latest/FileFormats/Overview.html>`_.

    :param geometry_file: the path to the input geometry file
    :param output_format: the desired output format
    :param geometry_format: the format of the input geometry file
    :return: formatted geometry
    """
    if geometry_format is None:
        geometry_format = os.path.basename(geometry_file).split(".")[1]

    obConversion = openbabel.OBConversion()
    obConversion.SetInAndOutFormats(geometry_format, output_format)

    mol = openbabel.OBMol()
    obConversion.ReadFile(mol, geometry_file)

    formatted_output = obConversion.WriteString(mol)

    if formatted_output == "":
        message = "Unsupported input geometry format: {}".format(geometry_format)
        raise AttributeError(message)

    return formatted_output


def get_energy(output_file: str, format: str, excited_state: bool = False) -> float:
    """
    Returns the energy from the given output file in units of electronvolts (eV).
    :param output_file: the path to the output file
    :param format: the format of the output file
    :param excited_state: whether to extract excited state energy
    :return: an energy in eV
    """
    if format == "gaussian16":
        energy_line = find_string(Path(output_file).resolve(), "SCF Done")[-1]
        energy = float(energy_line.split("A.U.")[0].split()[-1]) * 27.2113246
    elif format == "gamess":
        raise NotImplementedError("GAMESS get_energy not implemented")
    else:
        raise AttributeError("Unable to obtain energy from file format '{}'".format(format))
    return energy


def get_smiles(geometry_file: str, geometry_format: str = None) -> str:
    """
    Returns the SMILES string from the given geometry file. The format of the
    geometry file is assumed based on the filename extension but can be
    specified with the ``geometry_format`` keyword argument.

    For a list of supported geometry formats, refer to the `Open Babel documentation
    <https://open-babel.readthedocs.io/en/latest/FileFormats/Overview.html>`_.

    :param geometry_file: the path to the input geometry file
    :param geometry_format: the format of the geometry file
    :return: the SMILES string of the molecule
    """
    smiles = get_formatted_geometry(geometry_file,
                                    geometry_format=geometry_format,
                                    output_format="can").split()[0]

    return smiles


def get_supported_babel_formats(input: bool = True) -> List[str]:
    """
    Returns Open Babel's supported input or output formats.

    If ``input=True``, the supported input formats are returned;
    otherwise, the supported output formats are returned.

    .. seealso:: `Open Babel documentation <https://open-babel.readthedocs.io/en/latest/FileFormats/Overview.html>`_.

    :param input: whether to return input formats
    :return: a list of supported formats
    """
    obConversion = openbabel.OBConversion()
    if input:
        return obConversion.GetSupportedInputFormat()
    else:
        return obConversion.GetSupportedOutputFormat()


def valid_smiles(smiles: str) -> bool:
    """
    Determines if the given SMILES string represents a valid molecule.

    :param smiles: the SMILES string of the molecule
    :return: True if the SMILES string is valid, False otherwise

    """
    mol = Chem.MolFromSmiles(smiles)
    return mol is not None
