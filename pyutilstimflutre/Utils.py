# -*- coding: utf-8 -*-
# Utility functions (static methods of the Utils class)

# Copyright (C) 2014-2016 Institut National de la Recherche Agronomique (INRA)
# License: GPL-3+
# Persons: Timothée Flutre [cre,aut]
# Versioning: https://github.com/timflutre/pyutilstimflutre

from __future__ import print_function
from __future__ import unicode_literals

import random
import string
import subprocess
import sys

class Utils(object):
    
    def __init__(self):
        pass
    
    @staticmethod
    def user_input(msg):
        if sys.version_info[0] == 2:
            return raw_input(msg)
        elif sys.version_info[0] == 3:
            return input(msg)
        else:
            msg = "Python's major version should be 2 or 3"
            raise ValueError(msg)
        
    @staticmethod
    def uniq_alphanum(length):
        return "".join(random.choice(string.letters+string.digits) \
                       for i in xrange(length))
    
    @staticmethod
    def isProgramInPath(prgName):
        args = ["which", prgName]
        try:
            p = subprocess.check_output(args, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError, e:
            # msg = "can't find '%s' in PATH" % prgName
            # raise ValueError(msg)
            return False
        else:
            prgPath = p.rstrip()
            return True

    @staticmethod
    def getProgramPath(prgName):
        args = ["which", prgName]
        try:
            p = subprocess.check_output(args, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError, e:
            msg = "can't find '%s' in PATH" % prgName
            raise ValueError(msg)
        else:
            progPath = p.rstrip()
            return progPath
