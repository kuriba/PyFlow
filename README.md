# PyFlow: A Generalized Program for Running Custom Sequences of Quantum Chemistry Calculations using Slurm
PyFlow is a program designed to develop custom, modular, high-throughput quantum chemistry screening workflows to support the discovery of novel, sustainable materials. PyFlow offers significant flexibility and allows you to easily setup an automated workflow for computing ground or excited state molecular geometries and energies.

#### Prerequisites
- Access to a high-performance computing (HPC) cluster
- [Gaussian 16](https://gaussian.com/gaussian16/) and/or [GAMESS](https://www.msg.chem.iastate.edu/GAMESS/) version 2018-R1 or later
- [Anaconda](https://www.anaconda.com/) with Python 3.8+

## Installation
1. Clone this GitHub repository into your home directory on the cluster.
	```console
	git clone https://github.com/kuriba/PyFlow.git
	```
2. Go into the newly created `PyFlow` directory and set up an Anaconda environment using the provided environment.yml file.
	```console
    conda env create --file environment.yml
	```
3. Activate the newly created environment.
	```console
    conda activate pyflow
	```
4.  Run the following setup command within the PyFlow directory.
    ```console
    pip install -e .
    ```
5. Define the following environment variables in your .bashrc.*
    ```console
    export PYFLOW=~/PyFlow
    export SCRATCH=/path/to/your/scratch/
    ```
	\*_Note: replace_`/path/to/your/scratch/`_with the actual path to your scratch directory._
    
6. If you intend to use GAMESS, create a directory called `scr` within your scratch directory.

---

## Creating a custom workflow

### The workflow configuration file

The heart of workflow customizability is in the workflow configuration file. The workflow configuration file is a JSON-formatted file that defines the steps in a workflow and instructions for how to run each step. This file has some specific formatting requirements but has the general structure shown below.

```
{
  "default": {
    "initial_step": "X",
    "steps": {
      "X": {
        "program": "gaussian16",
        "route": "#p pm7 opt",
        "opt": true,
        "conformers": true,
        "dependents": ["Y"]
      },
      "Y": { ... },
      "Z": { ... }
    }
  },
  "alt": { ... }
}
```

The "default" key is a `config_id` that refers to a flow configuration. The second `config_id`, "alt", refers to another flow configuration. It's possible to define multiple flow configurations in a single file for the sake of organization. The desired workflow configuration can be selected at runtime. Each flow configuration is a JSON object that must specify two keys: `initial_step` and `steps`. The former declares the first step of the workflow, and the latter defines the specific instructions for running each step. Each step must define a JSON object of step parameters such as partition, memory, and time limits (see supported workflow step parameters below for an exhaustive list of supported step parameters). In the example configuration above, the defined steps are "X", "Y", and "Z".

#### Supported workflow step parameters
##### General step parameters 
The following step parameters are supported by all QC programs

| Parameter | Description | Data type | Default |
| :-------- | :---------- | :-------- | :------ |
| `program` | the QC program to use | `str` | `none` |
| `opt` | whether the step includes an optimization | `bool` | `true` |
| `freq` | whether the step includes a frequency calculation | `bool` | `false` |
| `single_point` | whether the step is a single-point calculation | `bool` | `false` |
| `conformers` | whether the step has conformers | `bool` | `false` |
| `proceed_on_failed_conf` | if `true`, allow molecules with failed conformers to proceed to the next step | `bool` | `true` |
| `attempt_restart` | whether to attempt to restart a calculation upon timeout or failure | `bool` | `false` |
| `nproc` | number of cores to request through Slurm | `int` | `14` |
| `memory` | amount of memory to request, in GB | `int` | `8` |
| `time` | the time limit for the calculation, in minutes | `int` | `1400` |
| `time_padding` | the time limit for processing/handling calculation outputs (the overall time limit for the Slurm submission is `time + time_padding`) | `int` | `5` |
| `partition` | the partition to request for the step | `str` | `short` |
| `simul_jobs` | the number of jobs to simultaneously run | `int` | `50` |
| `save_outputs` | whether to save the results of a step in /work/lopez/workflows | `bool` | `false` |
| `dependents` | a list of step IDs that are to be run after the completion of the current step | `List[string]` | `[]` |
| `charge` | the charge by which to increment all molecules | `int` | `0` |
| `multiplicity` | the multiplicity of the molecules | `int` | `1` |

##### Quantum chemistry program-specific step parameters*

<table>
	<tr>
		<td width="125"><b>QC program</b></td>
		<td><b>Parameter</b></td>
		<td><b>Description</b></td>
		<td width="100"><b>Data type</b></td>
		<td><b>Default</b></td>
	</tr>
	<tr>
		<td rowspan=3>gaussian16</td>
		<td><code>route</code></td>
		<td>the full route for the calculation</td>
		<td><code>str</code></td>
		<td><code>none</code></td>
	</tr>
	<tr>
		<td><code>rwf</code></td>
		<td>whether to save the .rwf file</td>
		<td><code>bool</code></td>
		<td><code>false</code></td>
	</tr>
	<tr>
		<td><code>chk</code></td>
		<td>whether to save the .chk file</td>
		<td><code>bool</code></td>
		<td><code>false</code></td>
	</tr>
	<tr>
		<td rowspan=8>gamess</td>
		<td><code>gbasis</code></td>
		<td>Gaussian basis set specification</td>
		<td><code>str</code></td>
		<td><code>none</code></td>
	</tr>
	<tr>
		<td><code>runtyp</code></td>
		<td>the type of computation (e.g., energy, gradient, etc.)</td>
		<td><code>str</code></td>
		<td><code>none</code></td>
	</tr>
	<tr>
		<td><code>dfttyp</code></td>
		<td>DFT functional to use (ab initio if unspecified)</td>
		<td><code>str</code></td>
		<td><code>none</code></td>
	</tr>
	<tr>
		<td><code>maxit</code></td>
		<td>maximum number of SCF iteration cycles</td>
		<td><code>int</code></td>
		<td><code>30</code></td>
	</tr>
	<tr>
		<td><code>opttol</code></td>
		<td>gradient convergence tolerance, in Hartree/Bohr</td>
		<td><code>float</code></td>
		<td><code>0.0001</code></td>
	</tr>
	<tr>
		<td><code>hess</code></td>
		<td>selects the initial Hessian matrix</td>
		<td><code>str</code></td>
		<td>depends on <code>runtyp</code> (see GAMESS documentation)</td>
	</tr>
	<tr>
		<td><code>nstep</code></td>
		<td>maximum number of steps to take</td>
		<td><code>int</code></td>
		<td>50 for minimum search, 20 for transition state search</td>
	</tr>
	<tr>
		<td><code>idcver</code></td>
		<td>the dispersion correction implementation to use</td>
		<td><code>int</code></td>
		<td><code>none</code></td>
	</tr>
</table>

*_refer to the documentation specific to each QC program for more details on valid arguments for each parameter_

### Workflow customization utility
It is possible to manually create a workflow configuration file in any text editor, but this places the burden of properly formatting the JSON file and including required step parameters on the user. To simplify the creation of properly-formatted workflow configuration files, the program includes the `build_config` utility for creating custom workflows via the command line. To access the utility, use the following command, replacing `new_config.json` and `default` with the desired configuration file name and configuration ID, respectively.
```console
pyflow build_config --config_file new_config.json --config_id default
```
You will see several prompts to enter step information and modify step parameters for your new workflow configuration (you can add a workflow configuration with a new ID to an existing configuration file by providing the path to the existing config file as the argument for `--config_file`).

---

## Setting up and running a workflow

Execution of a workflow is accomplished in three steps:
1. Setting up a directory for the workflow
2. Uploading molecules (as PDB files) to the `unopt_pdbs` folder of the workflow
3. Submitting the workflow

#### Setting up a workflow directory
To create a directory for your workflow, go to your scratch directory and run the following command, replacing `my_first_workflow` with your desired workflow name. The argument for the `config_file` flag should be the path to the desired workflow configuration file, and the argument for the `config_id` flag specifies which configuration to use from the specified configuration file.
```console
pyflow setup my_first_workflow --config_file /path/to/config/file --config_id "default"
```
#### Uploading molecules
Next, place the molecules for the workflow in the `unopt_pdbs` folder of the workflow directory that was created in the previous step. The structures should use the PDB format. The files should be named with the InChIKey of the molecule, followed by an underscore, followed by the conformer ID*, starting from 0.
```
XXXXXXXXXXXXXX-YYYYYYYYYY-Z_0.pdb
XXXXXXXXXXXXXX-YYYYYYYYYY-Z_1.pdb
XXXXXXXXXXXXXX-YYYYYYYYYY-Z_2.pdb
XXXXXXXXXXXXXX-YYYYYYYYYY-Z_3.pdb
```
*_Note: If you only have one conformer for each molecule, the PDB files should each have the conformer ID "0"._
#### Submitting the workflow
To submit the workflow, run the following command while you're located in the workflow directory. This command will set up the input files for the first step using the initial coordinates from the structures in the `unopt_pdbs` folder, then submit them as an array.
```console
pyflow begin
```
#### Progress monitoring
The `progress` command is provided for easily monitoring the progress of a workflow. To use it, simply go to the directory of a running or completed workflow and execute the following command. This will output a small report on the overall progress of the calculations.
```console
pyflow progress
```

---

## File generation utilities
To simplify the generation of Gaussian 16 input files and Slurm submission scripts, these utilities are accessible as their own actions: `g16` and `sbatch`, respectively. Below you'll find several examples which demonstrate how to use these utilities to generate files.

### Gaussian 16 input files
Generating a Gaussian 16 input file requires two arguments: a route and a geometry file (for the initial coordinates).

In this first example, a Gaussian 16 input file named `file.com` will be generated with the coordinates from file.pdb and the route "#p pm7 opt". This example uses default values for the charge (0), multiplicity (1), nproc (14), and memory (8 GB).
```console
pyflow g16 -r "#p pm7 opt" -g /path/to/geometry/file.pdb
```
It is possible to specify the charge, multiplicity, memory and CPU allocation as follows.
```console
pyflow g16 -r "#p pm7 opt" -g /path/to/geometry/file.pdb --charge 1 --multiplicity 3 --memory 16 --nproc 16
```
The file generator attempts to determine the format of the initial geometry file based on its file ending (pdb in the examples above). If the file ending does not match a known Open Babel format, you can specify the format with the `--geometry_format` flag (refer to the [Open Babel documentation](https://open-babel.readthedocs.io/en/latest/FileFormats/Overview.html) for a complete list of supported formats).
```console
pyflow g16 -r "#p pm7 opt" -g /path/to/geometry/file.o --geometry_format xyz
```
_Note: use `pyflow g16 --help` for an exhaustive list of options available for generating Gaussian 16 input files._

### Slurm submission scripts
Generating Slurm submission scripts requires two arguments: a jobname and a file with commands to run.

In this example, a Slurm submission script named `generic_slurm_job.sbatch` will be generated with the commands in the commands.txt text file.
```console
pyflow sbatch -j generic_slurm_job -c /path/to/commands.txt
```

A number of arguments can be used to customize the Slurm submission script. In the example below, the partition, time limit (in minutes), number of nodes, and memory per node (in GB) are specified.
```console
pyflow sbatch -j another_generic_job -c /path/to/commands.txt --partition lopez --time 2880 --nodes 2 --memory 64
```

It is also possible to generate a submission script for an array with the `--array` flag. In the following example, a Slurm array submission script will be generated with 500 jobs in the array limited to 40 simultaneously running jobs.

```console
pyflow sbatch -j generic_array_job -c /path/to/commands.txt --array 500 --simul_jobs 40
```

_Note: use `pyflow sbatch --help` for an exhaustive list of options available for generating Slurm submission scripts._

---

#### Acknowledgements

   Prof. Steven A. Lopez  
   Dr. Jordan Cox  
   Daniel Adrion  
   Fatemah Mukadum  
   Patrick Neal  
 
---
#### External links
   [Lopez Lab website](https://web.northeastern.edu/lopezlab)   
   [VERDE Materials DB](https://doi.org/10.18126/8v3wxz72)   
