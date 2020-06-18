from pathlib import Path
from typing import List


def remove_file(filepath: str, force: bool = False, message: str = None) -> bool:
    """
    Removes the specified file by first prompting the user with a yes/no query.
    If ``force == True``, the file is removed without prompting the user.

    :param filepath: path to the file to remove
    :param force: remove file without prompting user
    :param message: message with which to prompt user
    :return: a boolean indicating whether the file was removed
    """
    if message is None:
        message = "Do you wish to remove this file: {}? [y/n]:\n"

    filepath = Path(filepath)

    if filepath.exists() and force:  # remove file if force option specified
        filepath.unlink()
    elif filepath.exists():  # ask to remove file if force is not specified
        remove = yes_no_query(message.format(filepath))
        if remove:
            filepath.unlink()
        else:
            return False
    return True


def upsearch(filename: str, start_dir: str = ".", message: str = None) -> Path:
    """
    Recursively searches upwards for the given file or directory name and
    returns the path to the file or directory if it is found.

    :param filename: the name of the file or directory to search for
    :param start_dir: the directory from which to start searching
    :param message: the error message to provide the user with if the file or directory is not found
    :return: the path to the file or directory
    :raises FileNotFoundError: if the file or directory is not found
    """
    cwd = Path(start_dir).resolve()

    while True:
        dirs_and_files = [d.name for d in list(cwd.glob('*'))]

        if filename in dirs_and_files:
            found_path = cwd / filename
            return found_path
        else:
            if cwd == cwd.parent:
                if message is None:
                    raise FileNotFoundError("Error: '{}' not found".format(filename))
                else:
                    raise FileNotFoundError(message.format(filename))
            else:
                cwd = cwd.parent


def find_string(filepath: Path, search_string: str) -> List[str]:
    """
    Searches for the given ``search_string`` in the file at the given path.

    :param filepath: the path to the file to search
    :param search_string: the string to search for
    :return: a list of lines with matches
    :raises FileNotFoundError: if the given file does not exist
    """
    if filepath.is_file():
        matches = []

        with filepath.open() as f:
            for line in f:
                if search_string in line:
                    matches.append(line)

        return matches

    else:
        raise FileNotFoundError("The file {} does not exist.".format(filepath))


def yes_no_query(query: str) -> bool:
    """
    Performs a command line, yes/no query and returns ``True`` or ``False`` if
    the answer is yes or no respectively.

    :param query: the question to ask the user
    :return: True or False if the user replies yes or no, respectively
    """
    # valid answers
    valid = {"yes": True, "ye": True, "y": True,
             "no": False, "n": False}

    while True:
        answer = input(query).lower()
        if answer in valid:
            return valid[answer]
        else:
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")
