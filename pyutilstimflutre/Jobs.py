# -*- coding: utf-8 -*-
# Manage jobs on a computer cluster running SGE

# Copyright (C) 2014-2016 Institut National de la Recherche Agronomique (INRA)
# License: GPL-3+
# Persons: Timothée Flutre [cre,aut]
# Versioning: https://github.com/timflutre/pyutilstimflutre

# TODO:
# - allow to specify Job.duration and Job.memory
# - record return status of a job
# - allow to use out-of-memory system to store jobs (e.g. SQLite)

from __future__ import print_function
from __future__ import unicode_literals

import os
import subprocess
import sys
import time

class Job(object):
    
    def __init__(self, groupId, name, cmd=None, bashFile=None, dir=None):
        self.groupId = groupId
        self.name = name
        self.cmd = cmd # should be cmd or bashFile, not both
        self.bashFile = bashFile # should be an absolute path
        self.dir = dir # directory in which the output of "qsub -N" should be
        self.queue = None # set by JobGroup upon insertion
        self.duration = None # set by JobGroup upon insertion
        self.memory = None # set by JobGroup upon insertion
        self.id = None # set right after submission
        self.node = None # not used yet
        
    def submit(self):
        cwd = os.getcwd()
        if self.dir:
            os.chdir(self.dir)
            
        qsubCmd = "qsub -cwd -j y -V"
        qsubCmd += " -q %s" % self.queue
        qsubCmd += " -N %s" % self.name
        if self.duration:
            pass
        if self.memory:
            pass
        
        cmd = ""
        if self.bashFile:
            if not os.path.exists(self.bashFile):
                msg = "can't find file '%s'" % self.bashFile
                raise ValueError(msg)
            cmd += "%s %s" % (qsubCmd, self.bashFile)
        elif self.cmd:
            cmd += "echo -e '%s'" % self.cmd.encode('unicode-escape')
            cmd += " | %s" % qsubCmd
        else:
            msg = "try to submit job '%s' with neither cmd nor bash file" \
                  % self.name
            raise ValueError(msg)
        
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
        p = p[0].split()[2]
        self.id = int(p)
        
        os.chdir(cwd)
        
        return self.id
    
    
class JobGroup(object):
    
    def __init__(self, groupId, scheduler, queue):
        self.id = groupId
        self.scheduler = scheduler
        self.checkScheduler()
        self.queue = queue
        self.checkQueue()
        self.lJobs = [] # filled via self.insert()
        self.lJobNames = [] # filled via self.insert()
        self.lJobIds = [] # filled via self.submit()
        
    def checkScheduler(self):
        if self.scheduler not in ["SGE"]:
            msg = "unknown scheduler '%s'" % self.scheduler
            raise ValueError(msg)
        
    def checkQueue(self):
        if self.scheduler == "SGE":
            p = subprocess.Popen(["qconf", "-sql"], shell=False, stdout=subprocess.PIPE).communicate()
            p = p[0].split("\n")
            if self.queue not in p:
                msg = "unknown queue '%s'" % self.queue
                raise ValueError(msg)
            
    def insert(self, iJob):
        self.lJobs.append(iJob)
        self.lJobs[-1].scheduler = self.scheduler
        self.lJobs[-1].queue = self.queue
        self.lJobNames.append(iJob.name)
        
    def submit(self, lIdxJobs=None):
        if not lIdxJobs: # launch all jobs by default
            lIdxJobs = range(len(self.lJobs))
        for i in lIdxJobs:
            jobId = self.lJobs[i].submit()
            self.lJobIds.append(jobId)
            
    def getUnfinishedJobIds(self, method="oneliner"):
        if method not in ["oneliner", "xml"]:
            msg = "unknown method '%s'" % method
            raise ValueError(msg)
        
        lUnfinishedJobIds = []
        cmd = "qstat -u '%s'" % os.getlogin()
        cmd += " -q %s" % self.queue
        
        if method == "oneliner":
            cmd += " | sed 1,2d"
            cmd += " | awk '{print $1}'"
            # print(cmd) # debug
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
            p = p[0].split("\n")[:-1]
            # print(p) # debug
            lUnfinishedJobIds = [int(jobId) for jobId in p
                                 if int(jobId) in self.lJobIds]
        elif method == "xml": # http://stackoverflow.com/a/26104540/597069
            cmd += " -r -xml"
            f = os.popen(cmd)
            dom = xml.dom.minidom.parse(f)
            jobs = dom.getElementsByTagName('job_info')
            print(jobs) # debug
            for job in jobs:
                jobname = job.getElementsByTagName('JB_name')[0].childNodes[0].data
                jobown = job.getElementsByTagName('JB_owner')[0].childNodes[0].data
                jobstate = job.getElementsByTagName('state')[0].childNodes[0].data
                jobnum = job.getElementsByTagName('JB_job_number')[0].childNodes[0].data
                if jobname in self.lJobNames:
                    lUnfinishedJobIds.append(jobnum)
            print(lUnfinishedJobIds) # debug
            
        return lUnfinishedJobIds
    
    def wait(self, verbose=1):
        if verbose > 0:
            msg = "nb of jobs: %i (first=%i last=%i)" % (len(self.lJobIds),
                                                         self.lJobIds[0],
                                                         self.lJobIds[-1])
            sys.stdout.write("%s\n" % msg)
            sys.stdout.flush()
        time.sleep(2)
        if len(self.getUnfinishedJobIds()) == 0:
            return
        time.sleep(5)
        if len(self.getUnfinishedJobIds()) == 0:
            return
        time.sleep(10)
        if len(self.getUnfinishedJobIds()) == 0:
            return
        time.sleep(30)
        if len(self.getUnfinishedJobIds()) == 0:
            return
        while True:
            time.sleep(60)
            if len(self.getUnfinishedJobIds()) == 0:
                return
            
            
class JobManager(object):
    
    def __init__(self, projectId):
        self.projectId = projectId
        self.dbPath = ""
        self.groupId2group = {}
        
    def __getitem__(self, jobGroupId):
        return self.groupId2group[jobGroupId]
    
    def insert(self, iJobGroup):
        self.groupId2group[iJobGroup.id] = iJobGroup
