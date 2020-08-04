import glob
import os
from pathlib import Path
from typing import List

import numpy as np

from pyflow.io.io_utils import find_string


class GaussianRestarter:

    def __init__(self, input_file: Path, output_file: Path, **kwargs):
        self.input_file = input_file
        self.output_file = output_file

        self.route = GaussianRestarter.get_route(input_file)
        self.args = {}
        for k, v in kwargs.items():
            if k not in self.args and k != "route":
                self.args[k] = v

    @staticmethod
    def get_route(input_file: Path) -> str:
        """

        :return:
        """
        matches = find_string(input_file, "#")
        if len(matches) == 0:
            raise AttributeError("Unable to find route in input file: {}".format(input_file))
        else:
            return matches[0].strip()

    def get_opt_options(self) -> List[str]:
        opt_keyword = self.get_opt_keyword()
        try:
            opt_options = opt_keyword.split("=", 1)[1].replace(")", "").replace("(", "").split(",")
            return opt_options
        except:
            return []

    # returns the opt keyword
    def get_opt_keyword(self):
        opt_keyword = [i for i in self.route.split(" ") if i.startswith("opt")][0]
        return opt_keyword

    # returns a list of SCF energies from the given opt log file
    def get_scf_energies(self):
        scf_energies = find_string(self.output_file, "SCF Done:")
        energies = [float(line.split("=")[1].split('A.U.')[0].strip()) for line in scf_energies]
        return energies

    # determines if the SCF energy is oscillating
    def is_opt_oscillating(self):
        energy_diffs = np.diff(self.get_scf_energies())
        energy_stdev = np.std(energy_diffs)
        if energy_stdev >= 0.001:
            return True
        else:
            return False

    # determines if the given job needs to be restarted
    def needs_restart(self):
        normal_t_count = len(find_string(self.output_file, "Normal termination"))
        if "opt" in self.route and "freq" in self.route:
            return normal_t_count < 2
        elif "opt" in self.route or "freq" in self.route or "# Restart" in self.route:
            return normal_t_count < 1
        return False

    # determines if the given job encountered an error fail
    def error_fail(self):
        return len(find_string(self.output_file, "Error termination")) > 0

    # determines if the given job encountered a link 9999 failure
    def link_9999_fail(self):
        return len(find_string(self.output_file, "Error termination request processed by link 9999.")) > 0

    # determines if the given job failed due to a convergence failure
    def convergence_fail(self):
        return len(find_string(self.output_file, "Convergence failure -- run terminated.")) > 0

    # determines if the given job failed due to a FormBX failure
    def formbx_fail(self):
        return len(find_string(self.output_file, "FormBX had a problem.")) > 0

    # removes duplicate options from opt keyword
    def clean_options(self, opt_options: List[str]) -> List[str]:
        cleaned_options = []

        opt_options = [opt.lower() for opt in opt_options]
        if any([i.startswith("recalcfc") for i in opt_options]) and "calcfc" in opt_options:
            opt_options.remove("calcfc")

        for option in opt_options:
            if "=" in option:
                option_name = option.split("=")[0]
            else:
                option_name = option

            if not any([i.startswith(option_name) for i in cleaned_options]):
                cleaned_options.append(option)

        return cleaned_options

    # sets up an optimization to be restarted
    def restart_opt(self, additional_opt_options: List = None):
        if additional_opt_options is None:
            additional_opt_options = []
        else:
            additional_opt_options = [opt.lower() for opt in additional_opt_options]

        opt_keyword = self.get_opt_keyword()
        opt_options = self.get_opt_options()

        opt_options += additional_opt_options
        opt_options = self.clean_options(opt_options)

        new_opt_keyword = "opt=({})".format(",".join(opt_options))
        new_route = self.route.replace(opt_keyword, new_opt_keyword)

        return new_route

    # sets up a frequency calculation to be restarted
    def restart_freq(self) -> str:
        return "# Restart"

    # Removes the rwf files associated with the given log file
    def clear_gau_files(self):
        with self.output_file.open() as f:
            for line in f:
                line = line.strip()
                if "/Gau-" in line:
                    inp_id = line.split()[1].strip("\"").split("/")[-1].split("-")[1][:5]
                elif line.startswith("Entering Link 1"):
                    p_id = line.split()[-1].replace(".", "")

                if p_id is not None and inp_id is not None:
                    break

        for id in [p_id, inp_id]:
            for f in glob.glob("Gau-{}*".format(id)):
                os.remove(f)

    def get_new_route(self):
        if not self.output_file.exists():
            return None

        if self.needs_restart() and not self.error_fail():
            self.clear_gau_files()

            normal_t_count = len(find_string(self.output_file, "Normal termination"))
            if "opt" in self.route and "freq" in self.route:
                if normal_t_count == 1:
                    return self.restart_freq()
                elif normal_t_count == 0:
                    return self.restart_opt()
            elif "opt" in self.route:
                return self.restart_opt()
            elif "freq" in self.route:
                return self.restart_freq()

        elif self.convergence_fail() or self.formbx_fail() or self.link_9999_fail():
            self.clear_gau_files()
            return self.restart_opt(additional_opt_options=["recalcfc=4"])
