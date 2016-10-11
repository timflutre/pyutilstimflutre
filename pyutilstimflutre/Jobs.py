# -*- coding: utf-8 -*-
# Manage jobs on a computer cluster

# Copyright (C) 2014-2016 Institut National de la Recherche Agronomique (INRA)
# License: GPL-3+
# Persons: Timothée Flutre [cre,aut]
# Versioning: https://github.com/timflutre/pyutilstimflutre

# TODO:
# - allow to specify Job.duration and Job.memory
# - allow to submit a job array made of many similar jobs

from __future__ import print_function
from __future__ import unicode_literals

import os
import pwd
import subprocess
import sys
import time
import stat

from pyutilstimflutre import Utils, DbSqlite


class JobManager(object):
    """
    All job groups of a given job manager will share the same scheduler.
    """
    
    def __init__(self, scheduler, projectId):
        self.checkScheduler(scheduler)
        self.scheduler = scheduler # SGE
        self.projectId = projectId
        self.groupId2group = {} # key=identifier value=object
        self.path2db = "%s/%s_%s.db" % (os.getcwd(), self.projectId,
                                        Utils.uniq_alphanum(5))
        self.db = DbSqlite(self.path2db)
        self.setUpJobTable()
        
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
            
    def setUpJobTable(self):
        """
        The "status" column can take three different values: waiting, success, and error.
        Right after submission, it will be "waiting".
        Once it is not in the output of "qstat" anymore, the output file will be scanned.
        Depending on the result, the status will be updated to "success" or "error".
        """
        cmd = "CREATE TABLE jobs"
        cmd += " (jobid INT,"
        cmd += " jobname TEXT NOT NULL,"
        cmd += " jobdir TEXT NOT NULL,"
        cmd += " groupid TEXT NOT NULL,"
        cmd += " queue TEXT NOT NULL,"
        cmd += " resources TEXT,"
        cmd += " status TEXT NOT NULL,"
        cmd += " datetime TEXT DEFAULT CURRENT_TIMESTAMP NOT NULL)"
        self.db.execomm(cmd)
        
    def insert(self, iJobGroup):
        JobManager.checkQueue(self.scheduler, iJobGroup.queue)
        self.groupId2group[iJobGroup.id] = iJobGroup
        self.groupId2group[iJobGroup.id].scheduler = self.scheduler
        
    def submit(self, jobGroupId):
        self.groupId2group[jobGroupId].submit(self.db)
        
    def wait(self, jobGroupId, verbose=1):
        self.groupId2group[jobGroupId].wait(self.db, verbose)
        
    def close(self):
        self.db.conn.close()
        os.remove(self.path2db)
        
        
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
        self.dJobId2JobIdx = {} # filled via self.submit()
        
    def insert(self, iJob):
        self.lJobs.append(iJob)
        self.lJobs[-1].queue = self.queue
        self.lJobs[-1].lResources = self.lResources
        
    def submit(self, db):
        for i in range(len(self.lJobs)):
            jobId = self.lJobs[i].submit(self.scheduler, self.queue,
                                         db, self.lResources)
            self.dJobId2JobIdx[jobId] = i
            
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
                if jobnum in self.dJobId2JobIdx.keys():
                    lUnfinishedJobIds.append(jobnum)
            print(lUnfinishedJobIds) # debug
            
        return lUnfinishedJobIds
    
    def removeUnknownJobIds(self, lUnfinishedJobIds):
        """
        Remove job IDs belonging to the same user on the same queue but having another group ID.
        """
        lKnownUnfinishedJobIds = []
        for jobId in lUnfinishedJobIds:
            if jobId in self.dJobId2JobIdx:
                lKnownUnfinishedJobIds.append(jobId)
        return lKnownUnfinishedJobIds
    
    def updateStatusOfFinishedJobs(self, lUnfinishedJobIds, db):
        # retrieve job ids of finished jobs (i.e. not in qstat anymore)
        # whose status in the table still is "waiting"
        cmd = "SELECT jobid FROM jobs WHERE groupid=\"%s\"" % self.id
        cmd += " AND status=\"waiting\""
        if len(lUnfinishedJobIds) > 0:
            cmd += " AND jobid NOT IN (%s)" % ",".join([str(jobId) for jobId \
                                                        in lUnfinishedJobIds])
        db.cur.execute(cmd)
        lFinishedWaitingJobIds = db.cur.fetchall()
        lFinishedWaitingJobIds = [i[0] for i in lFinishedWaitingJobIds]
        
        if len(lFinishedWaitingJobIds) > 0:
            # for each of them, scan stdout+err, and set their new status
            for jobId in lFinishedWaitingJobIds:
                iJob = self.lJobs[self.dJobId2JobIdx[jobId]]
                stdoutFile = "%s/%s.o%s" % (iJob.dir, iJob.name, jobId)
                stdoutHandle = open(stdoutFile, "r")
                lastLine = stdoutHandle.readlines()[-1].rstrip()
                stdoutHandle.close()
                expected = "END OF job %s from group %s" % (iJob.name,
                                                            iJob.groupId)
                if lastLine == expected:
                    iJob.updateStatusIntoDb(db, "success")
                else:
                    iJob.updateStatusIntoDb(db, "error")
                    msg = "failure of job %s (group=%s, id=%s)" % \
                          (iJob.name, iJob.groupId, iJob.id)
                    msg += "\nlook into %s" % iJob.dir
                    raise ValueError(msg)
                
    def wait(self, db, verbose=1):
        if verbose > 0:
            msg = "nb of jobs: %i (first=%i last=%i)" % (len(self.lJobs),
                                                         self.lJobs[0].id,
                                                         self.lJobs[-1].id)
            sys.stdout.write("%s\n" % msg)
            sys.stdout.flush()
            
        for x in [2, 2, 2, 5, 5, 5, 10, 10, 10]:
            time.sleep(x)
            lUnfinishedJobIds = self.getUnfinishedJobIds()
            lUnfinishedJobIds = self.removeUnknownJobIds(lUnfinishedJobIds)
            self.updateStatusOfFinishedJobs(lUnfinishedJobIds, db)
            if len(lUnfinishedJobIds) == 0:
                break
        while True:
            time.sleep(15)
            lUnfinishedJobIds = self.getUnfinishedJobIds()
            lUnfinishedJobIds = self.removeUnknownJobIds(lUnfinishedJobIds)
            self.updateStatusOfFinishedJobs(lUnfinishedJobIds, db)
            if len(lUnfinishedJobIds) == 0:
                break
            
        if verbose > 0:
            msg = "all job(s) finished (%i)" % len(self.lJobs)
            sys.stdout.write("%s\n" % msg)
            sys.stdout.flush()
            
            
class Job(object):
    
    def __init__(self, groupId, name, cmd=None, bashFile=None, dir=None):
        self.groupId = groupId
        self.name = name
        self.cmd = cmd # string, potentially multi-line, with bash commands
        self.bashFile = bashFile # absolute path; if not None, take precedence over self.cmd
        self.dir = dir # directory in which the output of "qsub -N" should be
        self.queue = None # set via JobGroup upon insertion or submission
        self.lResources = None # set by JobGroup upon insertion or submission
        self.id = None # set inside submit()
        self.node = None # not used yet
        self.exitStatus = None # not used yet
        
    def insertIntoDb(self, db):        
        lColNames = db.getColumnList("jobs")
        cmd = "INSERT INTO jobs"
        cmd += "(%s)" % ", ".join(lColNames[:(-1)])
        cmd += " VALUES"
        cmd += " (%s" % self.id
        cmd += ", '%s'" % self.name
        cmd += ", '%s'" % self.dir
        cmd += ", '%s'" % self.groupId
        cmd += ", '%s'" % self.queue
        if self.lResources:
            cmd += ", '%s'" % " ".join(self.lResources)
        else:
            cmd += ", ''"
        cmd += ", '%s')" % "waiting"
        db.execomm(cmd)
        
    def updateStatusIntoDb(self, db, status):
        cmd = "UPDATE jobs"
        cmd += " SET status=\"%s\"" % status
        cmd += " WHERE groupid=\"%s\"" % self.groupId
        cmd += " AND jobname=\"%s\"" % self.name
        cmd += " AND queue=\"%s\"" % self.queue
        db.execomm(cmd)
        
    def submit(self, scheduler, queue, db, lResources=None):
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
            bashHandle = open(self.bashFile, "w")
            txt = "#!/usr/bin/env bash"
            txt += "\nset -e"
            txt += "\nset -o pipefail"
            txt += "\ndate"
            txt += "\n%s" % self.cmd
            txt += "\ndate"
            txt += "\necho 'END OF job %s from group %s'" % (self.name,
                                                             self.groupId)
            bashHandle.write("%s\n" % txt)
            bashHandle.close()
            os.chmod(self.bashFile, stat.S_IREAD | stat.S_IEXEC)
            args = qsubArgs + [self.bashFile]
            out = subprocess.check_output(args)
        else:
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
            
        ## out -> Your job <job_id> ("<job_name>") has been submitted
        self.id = int(out.split()[2])
        
        self.insertIntoDb(db)
        
        os.chdir(cwd)
        
        return self.id
