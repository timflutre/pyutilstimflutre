# -*- coding: utf-8 -*-
# Parse the output from the SamTools FlagStat program
# https://github.com/samtools/samtools

# Copyright (C) 2014-2016 Institut National de la Recherche Agronomique (INRA)
# License: GPL-3+
# Persons: Timothée Flutre [cre,aut]
# Versioning: https://github.com/timflutre/pyutilstimflutre

from __future__ import print_function
from __future__ import unicode_literals

import os
import string

class SamtoolsFlagstat(object):
    """
    Parse the output of `samtools flagstats' (works with version 1.1).
    """
    
    @staticmethod
    def initListStats():
        lStats = []
        lStats.append({"id": "total",
                       "name": "total",
                       "qc.passed": None, "qc.failed": None})
        lStats.append({"id": "second",
                       "name": "secondary",
                       "qc.passed": None, "qc.failed": None})
        lStats.append({"id": "suppl",
                       "name": "supplimentary",
                       "qc.passed": None, "qc.failed": None})
        lStats.append({"id": "dupl",
                       "name": "duplicates",
                       "qc.passed": None, "qc.failed": None})
        lStats.append({"id": "map",
                       "name": "mapped",
                       "qc.passed": None, "qc.failed": None})
        lStats.append({"id": "pairedseq",
                       "name": "paired in sequencing",
                       "qc.passed": None, "qc.failed": None})
        lStats.append({"id": "r1",
                       "name": "read1",
                       "qc.passed": None, "qc.failed": None})
        lStats.append({"id": "r2",
                       "name": "read2",
                       "qc.passed": None, "qc.failed": None})
        lStats.append({"id": "proppaired",
                       "name": "properly paired",
                       "qc.passed": None, "qc.failed": None})
        lStats.append({"id": "itmatemap",
                       "name": "with itself and mate mapped",
                       "qc.passed": None, "qc.failed": None})
        lStats.append({"id": "single",
                       "name": "singletons",
                       "qc.passed": None, "qc.failed": None})
        lStats.append({"id": "matediffchr",
                       "name": "with mate mapped to a different chr",
                       "qc.passed": None, "qc.failed": None})
        lStats.append({"id": "matediffchrQ5",
                       "name": "with mate mapped to a different chr (mapQ>=5)",
                       "qc.passed": None, "qc.failed": None})
        return lStats
    
    @staticmethod
    def header2str():
        lStats = SamtoolsFlagstat.initListStats()
        txt = "%s.qc.passed" % lStats[0]["id"]
        txt += "\t%s.qc.failed" % lStats[0]["id"]
        for dStat in lStats[1:]:
            txt += "\t%s.qc.passed" % dStat["id"]
            txt += "\t%s.qc.failed" % dStat["id"]
        return txt
    
    def __init__(self, inFile):
        self.inFile = inFile
        if not os.path.exists(self.inFile):
            msg = "can't find file '%s'" % self.inFile
            raise ValueError(msg)
        self.lStats = SamtoolsFlagstat.initListStats()
        self.load()
        
    def load(self):
        inHandle = open(self.inFile, "r")
        lines = inHandle.readlines()
        inHandle.close()
        
        if len(lines) != 13:
            print(lines)
            msg = "file '%s' has %i lines instead of 13" % (self.inFile, len(lines))
            raise ValueError(msg)
        
        for idx,dStat in enumerate(self.lStats):
            if string.find(lines[idx], dStat["name"]) == -1:
                print(lines[idx])
                msg = "can't find '%s' on line %i of file '%s'" \
                      % (dStat["name"], idx + 1, self.inFile)
                raise ValueError(msg)
            toks = lines[idx].split(" ")
            if len(toks) < 4:
                msg = "output format of 'samtools flagstat' may have changed"
                msg += " for '%s' (lane %i)" % (dStat["name"], idx + 1)
                raise ValueError(msg)
            self.lStats[idx]["qc.passed"] = int(toks[0])
            self.lStats[idx]["qc.failed"] = int(toks[2])
            
    def getTxtToWrite(self):
        txt = "%i" % self.lStats[0]["qc.passed"]
        txt += "\t%i" % self.lStats[0]["qc.failed"]
        for dStat in self.lStats[1:]:
            txt += "\t%i" % dStat["qc.passed"]
            txt += "\t%i" % dStat["qc.failed"]
        return txt
