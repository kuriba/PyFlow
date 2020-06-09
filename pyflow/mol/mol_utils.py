import os
from openbabel import openbabel
from typing import List

from rdkit import Chem


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
    :param geometry_format: the input geometry file format
    :param output_format: the desired output format
    :return: formatted geometry
    """

    if not geometry_format:
        geometry_format = os.path.basename(geometry_file).split(".")[1]

    obConversion = openbabel.OBConversion()
    obConversion.SetInAndOutFormats(geometry_format, output_format)

    mol = openbabel.OBMol()
    obConversion.ReadFile(mol, geometry_file)

    formatted_output = obConversion.WriteString(mol)

    if formatted_output == "":
        raise Exception("Unsupported input geometry format: {}".format(geometry_format))

    return formatted_output


def get_energy(output_file: str, format: str):
    # TODO implement get_energy
    if format == "gaussian16":
        pass
    elif format == "gamess":
        pass
    else:
        raise NotImplementedError("Unable to obtain energy from file format '{}'".format(format))


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
