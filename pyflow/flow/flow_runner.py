import os
import shutil
import subprocess
from glob import glob
from linecache import getline
from pathlib import Path
from typing import List, Tuple

from tqdm import tqdm

from pyflow.flow.commands import Commands
from pyflow.flow.flow_config import FlowConfig
from pyflow.flow.flow_utils import get_num_conformers, load_workflow_params, WORKFLOW_PARAMS_FILENAME
from pyflow.io.gamess_writer import GamessWriter
from pyflow.io.gaussian_writer import GaussianWriter
from pyflow.io.io_utils import upsearch, find_string
from pyflow.io.sbatch_writer import SbatchWriter
from pyflow.mol.mol_utils import get_energy


class FlowRunner:
    """
    Class which handles running workflow steps.
    """

    PROGRAM_INFILE_EXTENSIONS = {"gaussian16": "com",
                                 "gamess": "inp"}

    PROGRAM_OUTFILE_EXTENSIONS = {"gaussian16": "log",
                                  "gamess": "o"}

    PROGRAM_OPENBABEL_IN_FORMATS = {"gaussian16": "com",
                                    "gamess": "inp"}

    PROGRAM_OPENBABEL_OUT_FORMATS = {"gaussian16": "log",
                                     "gamess": "gam"}

    PROGRAM_INPUT_WRITER = {"gaussian16": GaussianWriter,
                            "gamess": GamessWriter}

    PROGRAM_COMMANDS = {"gaussian16": "g16",
                        "gamess": "rungms"}

    def __init__(self,
                 current_step_id: str,
                 flow_config: FlowConfig = None,
                 workflow_dir: Path = None):
        """
        Constructs a FlowRunner object which handles setting up and submitting
        workflow steps. This involves setting up input files and submissions scripts
        and submitting them to the Slurm queue.

        :param current_step_id: the ID of the current workflow step
        :param flow_config: a workflow configuration object
        :param workflow_dir: the main directory of the workflow
        """
        if flow_config is None:
            workflow_params = load_workflow_params()
            config_file = workflow_params["config_file"]
            config_id = workflow_params["config_id"]
            self.flow_config = FlowConfig(config_file, config_id)
        else:
            self.flow_config = flow_config

        if workflow_dir is None:
            workflow_params_file = upsearch(WORKFLOW_PARAMS_FILENAME)
            self.workflow_dir = workflow_params_file.parent
        else:
            self.workflow_dir = workflow_dir

        self.current_step_id = current_step_id
        self.current_step_config = self.flow_config.get_step(current_step_id)
        self.current_step_dir = self.workflow_dir / self.current_step_id
        self.step_program = self.flow_config.get_step(current_step_id)["program"]

    def run(self, show_progress: bool = False) -> None:
        """
        Sets up the current workflow step by creating input files and submission scripts,
        and submits job for calculation.

        :param show_progress: displays progress bars in CLI if True
        :return: None
        """
        self.setup_input_files(show_progress)

        num_input_files = self._create_job_list_file()

        sbatch_file = self.setup_sbatch_file(array_size=num_input_files)

        job_id = sbatch_file.submit()

        # self.queue_dependents(job_id)

    def is_first_step(self) -> bool:
        """
        Determines if the current step (``self.current_step_id``) is the first step
        in the workflow.

        :return: True if the current step is the first, False otherwise
        """
        return self.current_step_id == self.flow_config.get_initial_step_id()

    def get_prev_step_id(self) -> str:
        """
        Returns the ID of the previous step.

        :return: the ID of the previous step
        """
        return self.flow_config.get_previous_step_id(self.current_step_id)

    def get_source_structures(self) -> List[Path]:
        """
        Returns a list of structure files that will be used to generate the
        input files for the current step. If the current step is the the first
        step in the workflow, the source structures are found in the ``unopt_pdbs``
        folder of the workflow. Otherwise, the source structures are found in the
        ``completed`` folder of the preceding step.

        :return: a list of paths to the source structures
        """
        if self.is_first_step():
            source_structures_path = self.workflow_dir / "unopt_pdbs"
            source_file_extension = "pdb"
            structure_files = list(source_structures_path.glob("*.{}".format(source_file_extension)))
        else:
            # determine the previous step
            prev_step_id = self.get_prev_step_id()
            prev_program = self.flow_config.get_step(prev_step_id)["program"]

            source_structures_path = self.workflow_dir / prev_step_id / "completed"
            source_file_extension = FlowRunner.PROGRAM_OUTFILE_EXTENSIONS[prev_program]

            structure_files = source_structures_path.glob("*_{}*.{}".format(prev_step_id, source_file_extension))

            structure_files = self.filter_conformers(list(structure_files))

        return structure_files

    def get_input_filenames(self, structure_files: List[Path], structure_dest: Path) -> List[Tuple[Path, Path]]:
        """
        Returns a list of 2-tuples where the first element is a Path object of an
        output file from the previous step, and the second element is a Path object
        of the corresponding input file for the next step in the workflow.

        :param structure_files: a list of output files from the previous step
        :param structure_dest: the destination directory for the new input files
        :return: a list of 2-tuples of Path objects
        """
        input_file_extension = FlowRunner.PROGRAM_INFILE_EXTENSIONS[self.step_program]

        input_filenames = []

        for structure in structure_files:
            inchi_key = structure.stem.split("_")[0]

            if self.current_step_config["conformers"]:
                conf_id = structure.stem.split("_")[-1]

                input_filename = structure_dest / "{}_{}_{}.{}".format(inchi_key,
                                                                       self.current_step_id,
                                                                       conf_id,
                                                                       input_file_extension)
            else:
                input_filename = structure_dest / "{}_{}.{}".format(inchi_key,
                                                                    self.current_step_id,
                                                                    input_file_extension)

            filename_and_structure = (input_filename, structure)
            input_filenames.append(filename_and_structure)

        return input_filenames

    def setup_input_files(self, show_progress: bool) -> None:
        """
        Sets up input files for the current workflow step.

        :param show_progress: displays progress bar in CLI if True
        :return: None
        """
        structure_files = self.get_source_structures()

        if self.is_first_step():
            source_structure_format = "pdb"
        else:
            prev_step_id = self.flow_config.get_previous_step_id(self.current_step_id)
            prev_program = self.flow_config.get_step(prev_step_id)["program"]
            source_structure_format = FlowRunner.PROGRAM_OPENBABEL_OUT_FORMATS[prev_program]

        input_writer = FlowRunner.PROGRAM_INPUT_WRITER[self.step_program]

        structure_dest = self.current_step_dir

        input_filenames = self.get_input_filenames(structure_files, structure_dest)

        if show_progress:
            input_filenames = tqdm(input_filenames, desc="Setting up input files...")

        for f in input_filenames:
            input_filename = f[0]
            source_geometry = f[1]
            input_writer = input_writer.from_config(self.current_step_config,
                                                    input_filename,
                                                    source_geometry,
                                                    source_structure_format)
            input_writer.write()

    def need_lowest_energy_confs(self) -> bool:
        """
        Determines if the lowest energy conformers need to be isolated at this
        point in the workflow.

        :return: True if the lowest energy conformers are necessary, False otherwise
        """
        try:
            prev_step_id = self.flow_config.get_previous_step_id(self.current_step_id)
            return self.flow_config.get_step(prev_step_id)["conformers"] and not \
                self.flow_config.get_step(self.current_step_id)["conformers"]
        except ValueError:
            return False

    def filter_conformers(self, source_files: List[Path]) -> List[Path]:
        """
        Filters the conformers based on various workflow step parameters. The
        list of Path objects is filtered by removing failed conformers and/or by
        removing all but the lowest energy conformers.

        :param source_files: a list of Path objects to filter
        :return: a filtered list of Path objects
        """
        if not self.current_step_config["proceed_on_failed_conf"]:
            source_files = self.remove_failed_confs(source_files)
        if self.need_lowest_energy_confs():
            source_files = self.get_lowest_energy_confs(source_files)
        return source_files

    def remove_failed_confs(self, source_files: List[Path]) -> List[Path]:
        """
        Returns a list of Path objects where the molecules for which all conformers
        have not successfully completed are removed.

        :param source_files: a list of Path objects from which to remove failed molecules
        :return: a filtered list of Path objects
        """
        num_conformers = get_num_conformers()
        if num_conformers == 1:
            return source_files
        elif num_conformers > 1:

            completed_confs = {}

            for f in source_files:
                inchi_key = f.stem.split("_")[0]

                if inchi_key not in completed_confs:
                    completed_confs[inchi_key] = 1
                else:
                    completed_confs[inchi_key] += 1

            filtered_source_files = []
            for f in source_files:
                inchi_key = f.stem.split("_")[0]
                if completed_confs[inchi_key] == num_conformers:
                    filtered_source_files.append(f)

            return filtered_source_files

    def get_lowest_energy_confs(self, source_files: List[Path]) -> List[Path]:
        """
        Returns a list of the lowest energy conformers from the given list of
        output files.

        :param source_files: list of Path objects
        :return: a list of Path objects to the lowest energy conformers
        """
        if not self.current_step_config["conformers"]:
            raise AttributeError("The step '{}' has no conformers".format(self.current_step_id))

        prev_step_id = self.get_prev_step_id()
        prev_program = self.flow_config.get_step(prev_step_id)["program"]
        source_file_ext = FlowRunner.PROGRAM_OUTFILE_EXTENSIONS[prev_program]

        # compile energies for the conformers
        uniq_inchi_keys = set()
        conf_energies = {}  # inchi_key: List[(conf_id, energy)]
        for f in source_files:
            stem = f.stem.split("_")
            inchi_key = stem[0]
            conf_id = stem[-1]

            uniq_inchi_keys.add(inchi_key)

            energy = get_energy(str(f), format=prev_program)

            if inchi_key in conf_energies:
                conf_energies[inchi_key].append((conf_id, energy))
            else:
                conf_energies[inchi_key] = [(conf_id, energy)]

        # determine the lowest energy conformers
        lowest_energy_confs = []
        for inchi_key, confs in conf_energies.items():
            sorted_confs = sorted(confs, key=lambda x: x[1])
            lowest_energy_conf_id = sorted_confs[0][0]
            lowest_energy_confs.append("{}_{}_{}.{}".format(inchi_key,
                                                            prev_step_id,
                                                            lowest_energy_conf_id,
                                                            source_file_ext))

        lowest_energy_source_files = []
        for f in source_files:
            if f.name in lowest_energy_confs:
                lowest_energy_source_files.append(f)

        return lowest_energy_source_files

    def setup_sbatch_file(self, array_size) -> SbatchWriter:
        """
        Creates an sbatch file for the current workflow step.

        :return: an SbatchWriter object
        """
        sbatch_filename = "{}.sbatch".format(self.current_step_id)
        sbatch_filepath = self.current_step_dir / sbatch_filename

        sbatch_commands = self.get_commands()

        jobname = "{}_{}".format(self.workflow_dir.name, self.current_step_id)

        sbatch_writer = SbatchWriter.from_config(step_config=self.current_step_config,
                                                 filepath=sbatch_filepath,
                                                 jobname=jobname,
                                                 array=array_size,
                                                 commands=sbatch_commands,
                                                 cores=self.current_step_config["nproc"],
                                                 time=self.current_step_config["timelim"])
        sbatch_writer.write()

        return sbatch_writer

    def get_commands(self) -> str:
        """
        Retrieves the command strings used to create the Slurm submission script
        for the current step. The commands make calls to ``pyflow`` to both run
        calculations and handle outputs.

        :return: a string of commands for run
        """
        run_command = Commands.get_run_command(self.current_step_id,
                                               self.current_step_config["timelim"])
        job_handling = Commands.get_handle_command(self.current_step_id)
        commands = [run_command, job_handling]

        command_string = "\n".join(commands)

        return command_string

    def _create_job_list_file(self) -> int:
        """
        Creates a file named input_files.txt in the current workflow step directory.
        This text file is used by the array submission script to determine which input
        files to run.

        :return: the number of jobs in the array
        """
        input_file_extension = FlowRunner.PROGRAM_INFILE_EXTENSIONS[self.step_program]
        input_files = self.current_step_dir.glob("*.{}".format(input_file_extension))
        input_files = [f.name for f in input_files]
        input_files_list = "\n".join(input_files)

        job_list_file = self.current_step_dir / "input_files.txt"

        job_list_file.write_text(input_files_list)

        return len(input_files)

    def queue_dependents(self):
        dependents = self.flow_config.get_dependents(self.current_step_id)
        for dependent in dependents:
            step_config = self.flow_config.get_step(dependent)

    @staticmethod
    def run_array_calc(step_id: str, timelimit: int = None) -> None:
        """
        Static method for running a calculation as part of an array. This method
        should only be called from within a Slurm array submission script as it
        relies on the ``$SLURM_ARRAY_TASK_ID`` environment variable to determine
        which array calculation to run.

        :param step_id: the step ID to run
        :param timelimit: time limit in minutes
        :return: None
        """
        flow_runner = FlowRunner(current_step_id=step_id)
        input_file = flow_runner.get_input_file()
        flow_runner.run_quantum_chem(input_file, timelimit)

    def get_input_file(self) -> Path:
        """
        Determines the input file based on the ``$SLURM_ARRAY_TASK_ID`` environment
        variable.

        :return: a Path object pointing to the input file
        """
        task_id = int(os.environ["SLURM_ARRAY_TASK_ID"])
        job_list_file = str(self.current_step_dir / "input_files.txt")
        input_file = Path(getline(job_list_file, task_id).strip()).resolve()
        return input_file

    def run_quantum_chem(self, input_file: Path, timelimit: int = None) -> None:
        """
        Runs a quantum chemistry calculation as a subprocess.

        :param input_file: the input file to run
        :param timelimit: time limit in minutes
        :return: None
        """
        qc_command = FlowRunner.PROGRAM_COMMANDS[self.step_program]
        working_dir = input_file.parent

        updated_env = self._update_qc_environment()

        if timelimit is not None:
            timelimit = timelimit * 60

        process = subprocess.run([qc_command, input_file],
                                 timeout=timelimit,
                                 cwd=working_dir,
                                 env=updated_env)

    def _update_qc_environment(self) -> dict:
        """
        Updates the current environment (``os.environ``) by adding additional,
        program-specific environment variables.

        :return: a dict of environment variables
        """
        env = os.environ.copy()

        if self.step_program == "gaussian16":
            pass
        elif self.step_program == "gamess":
            pass
        else:
            raise

        return env

    def is_complete(self, output_file: Path) -> bool:
        """
        Determines if the given output file completed successfully.

        :param output_file: a Path object pointing to the output file
        :return: True if successful, False otherwise
        :raises AttributeError: if this method doesn't support the current step program
        """
        if self.step_program == "gaussian16":
            output_filepath = Path(output_file).resolve()
            matches = find_string(output_filepath, "Normal termination")
            return len(matches) == sum([self.current_step_config["opt"], self.current_step_config["freq"]])
        elif self.step_program == "gamess":
            pass
        else:
            raise AttributeError("Unknown program: {}".format(self.step_program))

    @staticmethod
    def handle_array_output(step_id: str) -> None:
        flow_runner = FlowRunner(current_step_id=step_id)
        input_file = flow_runner.get_input_file()

        in_file_ext = FlowRunner.PROGRAM_INFILE_EXTENSIONS[flow_runner.step_program]
        out_file_ext = FlowRunner.PROGRAM_OUTFILE_EXTENSIONS[flow_runner.step_program]

        output_file = str(input_file).replace(in_file_ext, out_file_ext)
        output_file = Path(output_file).resolve()
        if flow_runner.is_complete(output_file):
            completed_dest = flow_runner.current_step_dir / "completed"

            # move completed input/output files
            for f in glob("{}*".format(output_file.with_suffix(""))):
                shutil.move(f, str(completed_dest))

            flow_runner._remove_array_files()

        elif flow_runner.current_step_config["attempt_restart"]:
            pass

    def _remove_array_files(self) -> None:
        """
        Removes .o and .e files corresponding to the current ``$SLURM_ARRAY_JOB_ID``
        and ``$SLURM_ARRAY_TASK_ID``.

        :return: None
        """
        array_id = os.environ["SLURM_ARRAY_JOB_ID"]
        task_id = os.environ["SLURM_ARRAY_TASK_ID"]

        for ext in ["o", "e"]:
            f = Path("{}_{}.{}".format(array_id, task_id, ext))
            f.unlink()

    def restart_failed_calc(self):
        pass  # TODO implement restart for failed calcs
