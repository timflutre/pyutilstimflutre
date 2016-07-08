Package "pyutilstimflutre"
==========================

This directory contains the "pyutilstimflutre" package for the Python
programming language. This package contains Timothee Flutre's personal
Python code which can nevertheless be useful to others. The development is
funded by the Institut National de la Recherche Agronomique (INRA).

The copyright is owned by the INRA. See the COPYING file for usage
permissions.

The content of this directory is versioned using git, the central
repository being hosted on GitHub:
`https://github.com/timflutre/pyutilstimflutre
<https://github.com/timflutre/pyutilstimflutre>`_

I have invested a lot of time and effort in creating this package, please cite
it when using it for data analysis.

For users, when retrieving the package (as a tar.gz), you can install it from
the command-line via (no --upgrade the first time)::

$ pip install --upgrade --user pyutilstimflutre-<version>.tar.gz

For developpers, when editing the content of this repo, increment the version
of the package in `__init__.py` and execute the following commands::

$ python setup.py sdist
$ pip install --upgrade --user dist/pyutilstimflutre-<version>.tar.gz
