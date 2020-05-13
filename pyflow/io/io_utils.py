import os


class FileWriter:
    """
    Generalized class for writing text to a file.
    """

    def __init__(self, filename: str, location: str, text: str = "",
                 overwrite_mode: bool = False):
        """

        :param filename:
        :param location:
        :param text:
        :param overwrite_mode:
        """
        self.filename = filename
        self.location = location
        self.filepath = os.path.join(location, filename)
        self.text = text
        self.overwrite_mode = overwrite_mode

    def write(self):
        """
        Writes ``self.text`` to the file located at ``self.filepath``.

        :return: None
        """
        # check if file exists
        if os.path.isfile(self.filepath):
            message = "{} already exists. Do you wish to overwrite this file?" \
                      "[y/n]:\n".format(self.filepath)

            removed = remove_file(self.filepath, force=self.overwrite_mode,
                                  message=message)
        else:
            removed = True

        # write to file
        if removed:
            with open(self.filepath, "w") as f:
                f.write(self.text)

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


def remove_file(filename: str, force: bool = False, message: str = None) -> \
        bool:
    """
    Removes the specified file by first prompting the user with a yes/no query.
    If ``force == True``, the file is removed without prompting the user.

    :param filename: path to the file to remove
    :param force: remove file without prompting user
    :param message: message with which to prompt user
    :return: a boolean indicating whether the file was removed
    """
    if message is None:
        message = "Do you wish to remove this file: {}? [y/n]:\n"

    if force:  # remove file if force option specified
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass
    elif os.path.isfile(
            filename):  # ask to remove file if force is not specified
        remove = yes_no_query(message.format(filename))
        if remove:
            os.remove(filename)
        else:
            return False
    return True


def upsearch(filename: str) -> str:
    """
    Recursively searches upwards for the given file or directory name and
    returns the path to the file or directory if it is found.

    :param filename: the name of the file or directory to search for
    :return: the path to the file or directory
    :raises FileNotFoundError: if the file or directory is not found
    """
    cwd = os.getcwd()

    while True:
        parent_dir = os.path.dirname(cwd)
        if filename in os.listdir(cwd):
            found_path = os.path.join(cwd, filename)
            return found_path
        else:
            if cwd == parent_dir:
                raise FileNotFoundError(
                    "Error: '{}' not found".format(filename))
            else:
                cwd = parent_dir


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
