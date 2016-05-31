# -*- coding: utf-8 -*-
# Manage jobs on a computer cluster

# Copyright (C) 2014-2016 Institut National de la Recherche Agronomique (INRA)
# License: GPL-3+
# Persons: Timothée Flutre [cre,aut]
# Versioning: https://github.com/timflutre/pyutilstimflutre

# TODO:
# - allow to specify Job.duration and Job.memory
# - record exist status of a job, and stop if error
# - allow to use out-of-memory system to store jobs (e.g. SQLite)
# - allow to submit a job array made of many similar jobs

from __future__ import print_function
from __future__ import unicode_literals

import os
import pwd
import subprocess
import sys
import time
            
            
class JobManager(object):
    """
    All job groups of a given job manager will share the same scheduler.
    """
    
    def __init__(self, scheduler, projectId):
        self.checkScheduler(scheduler)
        self.scheduler = scheduler # SGE
        self.projectId = projectId
        self.groupId2group = {} # key=identifier value=object
        
    def __getitem__(self, jobGroupId):
        return self.groupId2group[jobGroupId]
    
    @staticmethod
    def checkScheduler(scheduler):
        if scheduler not in ["SGE"]:
            msg = "unknown scheduler '%s'" % scheduler
            raise ValueError(msg)
        
    @staticmethod
    def checkQueue(scheduler, queue):
        if scheduler == "SGE":
            p = subprocess.check_output(["qconf", "-sql"])
            p = p.split("\n")
            if queue not in p:
                msg = "unknown queue '%s'" % queue
                raise ValueError(msg)
            
    def insert(self, iJobGroup):
        JobManager.checkQueue(self.scheduler, iJobGroup.queue)
        self.groupId2group[iJobGroup.id] = iJobGroup
        self.groupId2group[iJobGroup.id].scheduler = self.scheduler
        
    def submit(self, jobGroupId):
        self.groupId2group[jobGroupId].submit()
        
    def wait(self, jobGroupId, verbose=1):
        self.groupId2group[jobGroupId].wait(verbose)
        
        
class JobGroup(object):
    """
    All jobs of a given job group will share the same queue and resources.
    """
    
    def __init__(self, groupId, queue, lResources=None):
        self.id = groupId
        self.scheduler = None # set by JobManager.insert()
        self.queue = queue # check by JobManager.insert()
        self.lResources = lResources
        self.lJobs = [] # filled via self.insert()
        self.lJobIds = [] # filled via self.submit()
        
    def insert(self, iJob):
        self.lJobs.append(iJob)
        self.lJobs[-1].queue = self.queue
        self.lJobs[-1].lResources = self.lResources
        
    def submit(self, lIdxJobs=None):
        if not lIdxJobs: # launch all jobs by default
            lIdxJobs = range(len(self.lJobs))
        for i in lIdxJobs:
            jobId = self.lJobs[i].submit(self.scheduler, self.queue,
                                         self.lResources)
            self.lJobIds.append(jobId)
            
    def getUnfinishedJobIds(self, method="oneliner"):
        if method not in ["oneliner", "xml"]:
            msg = "unknown method '%s'" % method
            raise ValueError(msg)
        
        lUnfinishedJobIds = []
        args = ["qstat"]
        args += ["-u", pwd.getpwuid(os.getuid())[0]]
        args += ["-q", self.queue]
        
        if method == "oneliner":
            p = subprocess.check_output(args)
            for line in p.split("\n")[2:]:
                tokens = line.split()
                if len(tokens) > 0:
                    lUnfinishedJobIds.append(int(tokens[0]))
        elif method == "xml":
            # http://stackoverflow.com/a/26104540/597069
            args += ["-r", "-xml"]
            cmd = " ".join(args)
            f = os.popen(cmd)
            dom = xml.dom.minidom.parse(f)
            jobs = dom.getElementsByTagName('job_info')
            print(jobs) # debug
            for job in jobs:
                jobname = job.getElementsByTagName('JB_name')[0].childNodes[0].data
                jobown = job.getElementsByTagName('JB_owner')[0].childNodes[0].data
                jobstate = job.getElementsByTagName('state')[0].childNodes[0].data
                jobnum = job.getElementsByTagName('JB_job_number')[0].childNodes[0].data
                if jobnum in self.lJobIds:
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
        for x in [2, 2, 10, 10, 20]:
            time.sleep(x)
            if len(self.getUnfinishedJobIds()) == 0:
                break
        while True:
            time.sleep(60)
            if len(self.getUnfinishedJobIds()) == 0:
                break
        if verbose > 0:
            msg = "all job(s) finished (%i)" % len(self.lJobIds)
            sys.stdout.write("%s\n" % msg)
            sys.stdout.flush()
            
            
class Job(object):
    
    def __init__(self, groupId, name, cmd=None, bashFile=None, dir=None):
        self.groupId = groupId
        self.name = name
        self.cmd = cmd # in submit(), should be cmd or bashFile, not both
        self.bashFile = bashFile # should be an absolute path
        self.dir = dir # directory in which the output of "qsub -N" should be
        self.queue = None # set via JobGroup upon insertion or submission
        self.lResources = None # set by JobGroup upon insertion or submission
        self.id = None # set inside submit()
        self.node = None # not used yet
        self.exitStatus = None # not used yet
        
    def submit(self, scheduler, queue, lResources=None):
        self.queue = queue
        self.lResources = lResources
            
        cwd = os.getcwd()
        if self.dir:
            os.chdir(self.dir)
            
        if scheduler == "SGE":
            qsubArgs = ["qsub"]
            qsubArgs += ["-cwd"]
            qsubArgs += ["-j", "y"]
            qsubArgs += ["-V"]
            qsubArgs += ["-q", self.queue]
            qsubArgs += ["-N", self.name]
            if self.lResources:
                for resource in self.lResources:
                    qsubArgs += ["-l", resource]
                    
        out = None
        if self.bashFile:
            if not os.path.exists(self.bashFile):
                msg = "can't find file '%s'" % self.bashFile
                raise ValueError(msg)
            args = qsubArgs + [self.bashFile]
            out = subprocess.check_output(args)
        elif self.cmd:
            echoArgs = ["echo"]
            echoArgs += ["-e", "'%s'" % self.cmd.encode('unicode-escape')]
            
            # see question http://stackoverflow.com/q/36006597/597069
            
            # http://stackoverflow.com/a/13332300/597069
            # echoProc = subprocess.Popen(echoArgs, stdout=subprocess.PIPE)
            # out = subprocess.check_output(qsubArgs, stdin=echoProc.stdout)
            # echoProc.wait()
            
            # http://stackoverflow.com/a/17129244/597069
            # echoProc = subprocess.Popen(echoArgs, stdout=subprocess.PIPE)
            # qsubProc = subprocess.Popen(qsubArgs, stdin=echoProc.stdout, stdout=subprocess.PIPE)
            # echoProc.stdout.close()
            # out = qsubProc.communicate()[0]
            # echoProc.wait()
            
            cmd = " ".join(echoArgs) + " | " + " ".join(qsubArgs)
            out = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            out = out.communicate()[0]
        else:
            msg = "try to submit job '%s' with neither cmd nor bash file" \
                  % self.name
            raise ValueError(msg)
        
        ## out -> Your job <job_id> ("<job_name>") has been submitted
        self.id = int(out.split()[2])
        
        os.chdir(cwd)
        
        return self.id
