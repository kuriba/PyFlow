import codecs
from os import path

from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))


def read(rel_path):
    here = path.abspath(path.dirname(__file__))
    with codecs.open(path.join(here, rel_path), 'r') as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


with open(path.join(here, 'README.md'), "r") as fh:
    long_description = fh.read()

setup(
    name="pyflow",
    version=get_version("pyflow/__init__.py"),
    author="Biruk Abreha",
    author_email="lopezlabassistant@gmail.com",
    description="A generalized, automated program for running custom sequences of quantum chemistry calculations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kuriba/PyFlow",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Chemistry"
    ],
    python_requires='>=3.8',
    entry_points={
        'console_scripts': [
            'pyflow = pyflow.flow.flow_action:main'
        ]
    }
)
