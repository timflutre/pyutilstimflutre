# -*- coding: utf-8 -*-
# Parse the output from the FastQC program
# http://www.bioinformatics.babraham.ac.uk/projects/fastqc/

# Copyright (C) 2014-2016 Institut National de la Recherche Agronomique (INRA)
# License: GPL-3+
# Persons: Timothée Flutre [cre,aut]
# Versioning: https://github.com/timflutre/pyutilstimflutre

from __future__ import print_function
from __future__ import unicode_literals

import os
import zipfile

class Fastqc(object):
    """
    Parse the output of `fastqc' (works with version 0.11.2).
    """
    
    @staticmethod
    def initListStats():
        lStats = []
        lStats.append({"id": "fastqc.version",
                       "name": "FastQC",
                       "value": None})
        
        # module 1: Basic Statistics
        lStats.append({"id": "basic.stats",
                       "name": "Basic Statistics",
                       "status": None,
                       "content": []})
        lStats[1]["content"].append({"id": "file.name",
                                     "name": "Filename",
                                     "value": None})
        lStats[1]["content"].append({"id": "file.type",
                                     "name": "File type",
                                     "value": None})
        lStats[1]["content"].append({"id": "encod",
                                     "name": "Encoding",
                                     "value": None})
        lStats[1]["content"].append({"id": "total.nb.sequences",
                                     "name": "Total Sequences",
                                     "value": None})
        lStats[1]["content"].append({"id": "seq.poor.qual",
                                     "name": "Sequences flagged as poor quality",
                                     "value": None})
        lStats[1]["content"].append({"id": "seq.len",
                                     "name": "Sequence length",
                                     "value": None})
        lStats[1]["content"].append({"id": "perc.gc",
                                     "name": "%GC",
                                     "value": None})
        
        # module 2: Per base sequence quality
        lStats.append({"id": "seq.qual.per.base",
                       "name": "Per base sequence quality",
                       "status": None,
                       "content": []})
        # TODO
        
        return lStats
    
    @staticmethod
    def header2str():
        lStats = Fastqc.initListStats()
        txt = ""
        # TODO
        return txt
    
    def __init__(self, zipFile):
        self.zipFile = zipFile # ZIP file format
        if not os.path.exists(self.zipFile):
            msg = "can't find file '%s'" % self.zipFile
            raise ValueError(msg)
        if not zipfile.is_zipfile(self.zipFile):
            msg = "file '%s' isn't a valid ZIP file" % self.zipFile
            raise ValueError(msg)
        self.root = os.path.splitext(os.path.basename(self.zipFile))[0]
        self.inFile = "fastqc_data.txt"
        self.lStats = Fastqc.initListStats()
        self.load()
        
    def load(self):
        iZip = zipfile.ZipFile(self.zipFile, "r")
        tmp = "%s/%s" % (self.root, self.inFile)
        if tmp not in iZip.namelist():
            msg = "'%s' not in '%s'" % (tmp, self.inFile)
            raise ValueError(msg)
        inHandle = iZip.open(tmp)
        lines = inHandle.readlines()
        inHandle.close()
        
        line = lines[0]
        tokens = line.rstrip().split("\t")
        if "FastQC" not in tokens[0]:
            msg = "first line of '%s/%s' should contain 'FastQC'" % \
                  (self.zipFile, self.inFile)
            raise ValueError(msg)
        self.lStats[0]["value"] = tokens[1]
        
        idxModule = 0
        idxModLine = 0
        for line in lines[1:]:
            if ">>" in line:
                if "END_MODULE" in line:
                    idxModLine = 0
                    if idxModule >= 1:
                        break
                else:
                    idxModule += 1
                    tokens = line.rstrip().replace(">>", "").split("\t")
                    if self.lStats[idxModule]["name"] != tokens[0]:
                        msg = "module #%i of '%s' should be '%s'" % \
                              (idxModule,
                               self.inFile,
                               self.lStats[idxModule]["name"])
                        raise ValueError(msg)
                    self.lStats[idxModule]["status"] = tokens[1]
            else:
                if line[0] == "#":
                    continue
                tokens = line.rstrip().split("\t")
                if idxModule == 1:
                    self.lStats[idxModule]["content"][idxModLine]["value"] = tokens[1]
                idxModLine += 1
                
    def getTxtToWrite(self):
        txt = ""
        # TODO
        return txt
