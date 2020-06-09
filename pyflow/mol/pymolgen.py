import argparse
import os
import sys
import time
from itertools import chain

from rdkit import Chem
from rdkit import rdBase
from rdkit.Chem import AllChem
from tqdm import tqdm

from pyflow.mol.mol_utils import valid_smiles

rdBase.DisableLog('rdApp.warning')

# TODO add comments

"""
This is a script which substitutes a given core molecule with the standard set
of spacers and linkers developed by Biruk Abreha and Steven Lopez. The script
will perform substituions on the core at position indicated using Uranium (U).
The conformers are written to PDB files in a folder named after the given
molecule name.

USAGE: python pymolgen.py molecule_name smiles
"""

# reaction SMILES for linkers
linker_rxns = {'unsubstituted': '[*:1]([U])>>[*:1]([H])',
               'benzene': '[*:1]([U])>>[*:1](c2ccc([Y])cc2)',
               'pyridine': '[*:1]([U])>>[*:1](c2ncc([Y])cc2)',
               'pyrimidine': '[*:1]([U])>>[*:1](c2ncc([Y])cn2)',
               'tetrazine': '[*:1]([U])>>[*:1](c2nnc([Y])nn2)',
               'cyclopentadiene': '[*:1]([U])>>[*:1]C2=CC=C([Y])C2',
               'pyrrole (2,5)': '[*:1]([U])>>[*:1](c2ccc([Y])N2)',
               'pyrrole (2,4)': '[*:1]([U])>>[*:1](c2cc([Y])cN2)',
               'pyrrole(N-methyl)': '[*:1]([U])>>[*:1](c2ccc([Y])N(C)2)',
               'pyrrole(N-COH)': '[*:1]([U])>>[*:1](c2ccc([Y])N(C=O)2)',
               'imidazole': '[*:1]([U])>>[*:1](c1cnc([Y])N1)',
               'furan': '[*:1]([U])>>[*:1]c2ccc([Y])O2',
               'thiophene': '[*:1]([U])>>[*:1]c2ccc([Y])S2',
               'thiophene(dioxide)': '[*:1]([U])>>[*:1](c2ccc([Y])S(=O)(=O)2)',
               'thiazole (2,5)': '[*:1]([U])>>[*:1](c2sc([Y])cn2)',
               'thiazole (2,4)': '[*:1]([U])>>[*:1](c2scc([Y])n2)',
               'oxazole (2,5)': '[*:1]([U])>>[*:1](c1ncc([Y])o1)',
               'oxazole (2,4)': '[*:1]([U])>>[*:1](c1nc([Y])co1)',
               'acetylene': '[*:1]([U])>>[*:1](C#C[Y])',
               'ethylene(trans)': '[*:1]([U])>>[*:1]/C=C/[Y]',
               'imine': '[*:1]([U])>>[*:1](C=N[Y])'}

# placeholder for linker addition
linker_placeholder = '[*:1]([U])'
linker_placeholder_mol = Chem.MolFromSmarts(linker_placeholder)

# reaction SMILES for terminal groups
terminal_rxns = {'hydrogen': '[*:1]([Y])>>[*:1]([H])',
                 'hydroxy': '[*:1]([Y])>>[*:1]([OH])',
                 'trifluoromethyl': '[*:1]([Y])>>[*:1][C](F)(F)F',
                 'trifluoromethoxy': '[*:1]([Y])>>[*:1][O][C](F)(F)F',
                 'methyl': '[*:1]([Y])>>[*:1][C]',
                 'methoxy': '[*:1]([Y])>>[*:1][O][C]',
                 'nitro': '[*:1]([Y])>>[*:1][N+]([O-])=O',
                 'thiol': '[*:1]([Y])>>[*:1]([SH])',
                 'fluoro': '[*:1]([Y])>>[*:1][F]',
                 'chloro': '[*:1]([Y])>>[*:1][Cl]',
                 'cyano': '[*:1]([Y])>>[*:1]C#N'}

terminal_placeholder = '[*:1]([Y])'
terminal_placeholder_mol = Chem.MolFromSmarts(terminal_placeholder)


# generates SMILES strings for the given core smiles
def generate_library(parent_smiles):
    parent_mol = Chem.MolFromSmiles(parent_smiles, sanitize=False)

    # append linkers to parent molecule to generate unsubstituted cores
    unsubstituted_cores = []
    place_holder_count = len(
        parent_mol.GetSubstructMatches(linker_placeholder_mol))
    for linker in linker_rxns:
        rxn = AllChem.ReactionFromSmarts(linker_rxns[linker])
        core = parent_mol
        for i in range(place_holder_count):
            new_mols = list(chain.from_iterable(rxn.RunReactants((core,))))
            core = new_mols[0]
            Chem.SanitizeMol(core)
        unsubstituted_cores.append(core)

    # append terminal groups
    all_mols = []
    for core in unsubstituted_cores:
        place_holder_count = len(
            core.GetSubstructMatches(terminal_placeholder_mol))
        if place_holder_count == 0:
            all_mols.append(core)
            continue
        for terminal in terminal_rxns:
            new_mol = core
            rxn = AllChem.ReactionFromSmarts(terminal_rxns[terminal])
            for i in range(place_holder_count):
                new_mols = list(
                    chain.from_iterable(rxn.RunReactants((new_mol,))))
                new_mol = new_mols[0]
                Chem.Cleanup(new_mol)
            all_mols.append(Chem.MolFromSmiles(Chem.MolToSmiles(new_mol)))

    # canonicalize smiles to remove duplicates
    all_mols = [Chem.MolFromSmiles(smiles) for smiles in
                [Chem.MolToSmiles(mol) for mol in all_mols]]
    all_smiles = list(set([Chem.MolToSmiles(mol) for mol in all_mols]))

    return all_smiles


# generates conformers for the given list of smiles strings
def generate_conformers(list_of_smiles, library_name, num_confs):
    # create directory to store molecules
    if not os.path.exists(library_name):
        os.makedirs(library_name)
    out_folder = os.path.abspath(library_name)

    # write list of SMILES to text file
    with open(os.path.join(out_folder, library_name + ".txt"), "w") as f:
        for smiles in list_of_smiles:
            f.write(smiles + "\n")

    start_time = time.time()

    good_conformers = 0
    no_rotatable_bonds = 0
    no_conformers = 0
    for smile in tqdm(list_of_smiles, desc="Generating conformers..."):
        print(smile)
        mol = Chem.AddHs(Chem.MolFromSmiles(smile))

        num_rotatable_bonds = AllChem.CalcNumRotatableBonds(mol)

        if num_rotatable_bonds == 0:
            target_num_confs = 1
        else:
            target_num_confs = num_confs

        # generate conformers
        num_generated_confs = 0
        rms_threshold = 0.005

        while num_generated_confs < target_num_confs and rms_threshold > 0:
            confs = AllChem.EmbedMultipleConfs(mol,
                                               numConfs=target_num_confs,
                                               useRandomCoords=False,
                                               pruneRmsThresh=rms_threshold,
                                               numThreads=0,
                                               useBasicKnowledge=True,
                                               forceTol=0.001)
            num_generated_confs = len(confs)
            rms_threshold -= 0.001
            print("RMS threshold updated to: {}".format(rms_threshold))

        # track number of successfully generated conformers
        if num_generated_confs == 0:
            no_conformers += 1
            continue
        elif num_generated_confs == 1:
            no_rotatable_bonds += 1
        else:
            good_conformers += target_num_confs

        # optimize conformers
        # opt = AllChem.UFFOptimizeMoleculeConfs(mol, numThreads=0, maxIters=1000, vdwThresh=10, ignoreInterfragInteractions=True)
        opt = AllChem.MMFFOptimizeMoleculeConfs(mol, numThreads=0, maxIters=10000, nonBondedThresh=10,
                                                ignoreInterfragInteractions=True)
        print(opt)

        # write conformers to PDB files
        inchi_key = Chem.InchiToInchiKey(Chem.MolToInchi(mol))
        for conf_id in range(num_generated_confs):
            conf_name = f"{inchi_key}_{conf_id}.pdb"
            pdb_file = os.path.join(out_folder, conf_name)
            pdb_writer = Chem.PDBWriter(pdb_file)
            pdb_writer.write(mol, conf_id)
            pdb_writer.close()

    # reporting
    time_taken = round(time.time() - start_time, 2)
    total_num_confs = no_rotatable_bonds + good_conformers

    return (total_num_confs, no_rotatable_bonds, no_conformers), time_taken


def main(args):
    # parse SMILES strings
    if args["smiles"]:
        if "." in args["smiles"][0]:  # split multiple molecule SMILES string
            parent_smiles = args["smiles"][0].split(".")
        else:
            parent_smiles = args["smiles"]
    elif args["file"]:
        parent_smiles = []
        with open(args["file"], "r") as f:
            for line in f:
                smiles = line.strip()
                if "." in smiles:
                    for s in smiles.split("."):
                        parent_smiles.append(s)
                else:
                    parent_smiles.append(smiles)

    # validate the SMILES strings
    invalid_smiles = list(filter(lambda x: not valid_smiles(x), parent_smiles))
    if invalid_smiles:
        message = "Error: the following {} SMILES strings are invalid:\n".format(
            len(invalid_smiles))
        for i in invalid_smiles:
            message += i + "\n"
        print(message.strip())
        sys.exit()

    # generate library
    all_smiles = []
    for smiles in tqdm(parent_smiles, desc="Generating molecules..."):
        new_smiles = generate_library(smiles)
        all_smiles += new_smiles

    # generate conformers
    report, time_taken = generate_conformers(all_smiles,
                                             args["name"],
                                             args["num_confs"])

    # reporting
    total_num_confs = report[0]
    no_rotatable_bonds = report[1]
    no_conformers = report[2]

    print("Successfully generated {} conformer(s) \
           in {} seconds.".format(total_num_confs, time_taken))
    if no_rotatable_bonds > 0:
        print("Generated one conformer for {} molecule(s) with no \
               rotatable bonds.".format(no_rotatable_bonds))
    if no_conformers > 0:
        print("Failed to generate conformers for {} \
               molecule(s).".format(no_conformers))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Script for generating HTVS libraries for VERDE Materials DB.")

    smiles_group = parser.add_mutually_exclusive_group(required=True)
    smiles_group.add_argument("-s", "--smiles",
                              type=str,
                              nargs=1,
                              help="the SMILES string of the core molecule")
    smiles_group.add_argument("-f", "--file",
                              type=str,
                              help="file with a SMILES string on each line")

    parser.add_argument("-n", "--name",
                        type=str,
                        required=True,
                        help="the name of the library")
    parser.add_argument("-c", "--num_confs",
                        type=int,
                        default=4,
                        help="number of conformers to generate for each unique molecule")

    args = vars(parser.parse_args())

    return args


if __name__ == "__main__":
    # parse arguments
    args = parse_args()

    # generate molecules
    main(args)
