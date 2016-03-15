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
import pwd
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
        self.lResources = None # set by JobGroup upon insertion
        self.id = None # set inside submit()
        self.node = None # not used yet
        
    def submit(self):
        cwd = os.getcwd()
        if self.dir:
            os.chdir(self.dir)
            
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
    
    
class JobGroup(object):
    
    def __init__(self, groupId, scheduler, queue, lResources=None):
        self.id = groupId
        self.scheduler = scheduler
        self.checkScheduler()
        self.queue = queue
        self.checkQueue()
        self.lResources = lResources
        self.lJobs = [] # filled via self.insert()
        self.lJobNames = [] # filled via self.insert()
        self.lJobIds = [] # filled via self.submit()
        
    def checkScheduler(self):
        if self.scheduler not in ["SGE"]:
            msg = "unknown scheduler '%s'" % self.scheduler
            raise ValueError(msg)
        
    def checkQueue(self):
        if self.scheduler == "SGE":
            p = subprocess.check_output(["qconf", "-sql"])
            p = p.split("\n")
            if self.queue not in p:
                msg = "unknown queue '%s'" % self.queue
                raise ValueError(msg)
            
    def insert(self, iJob):
        self.lJobs.append(iJob)
        self.lJobs[-1].scheduler = self.scheduler
        self.lJobs[-1].queue = self.queue
        self.lJobs[-1].lResources = self.lResources
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
