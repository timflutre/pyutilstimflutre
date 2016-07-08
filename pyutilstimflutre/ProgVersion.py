# -*- coding: utf-8 -*-
# Get the version of external programs

# Copyright (C) 2016 Institut National de la Recherche Agronomique (INRA)
# License: GPL-3+
# Persons: Timothée Flutre [cre,aut]
# Versioning: https://github.com/timflutre/pyutilstimflutre

# # Tests:
# $ python -m doctest ProgVersion.py

from __future__ import print_function
from __future__ import unicode_literals

import subprocess
import string

from pyutilstimflutre import Utils

class ProgVersion(object):
    """
    Get the version of external programs.
    """
    
    def __init__(self):
        pass
    
    @staticmethod
    def getVersion(binName):
        """
        Parse --version following http://www.gnu.org/s/help2man.
        
        >>> majVer, minVer = ProgVersion.getVersion("date")
        >>> type(majVer)
        <type 'int'>
        >>> type(minVer)
        <type 'int'>
        """
        args = [binName, "--version"]
        p = subprocess.check_output(args)
        p = p.splitlines()
        version = p[0].split(" ")[-1]
        majVer = int(version.split(".")[0])
        minVer = int(version.split(".")[1])
        return majVer, minVer
    
    @staticmethod
    def getVersionGatk(pathToJar=None):
        if pathToJar == None:
            pathToJar = Utils.getProgramPath("GenomeAnalysisTK.jar")
        args = ["java", "-Xmx1g", "-jar", pathToJar, "--version"]
        # p = subprocess.Popen(args, shell=True, stdout=subprocess.PIPE).communicate()
        cmd = " ".join(args)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
        version = p[0].split("-")[0]
        majVer = int(version.split(".")[0])
        minVer = int(version.split(".")[1])
        return majVer, minVer
