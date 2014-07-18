#!/usr/bin/env python

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This program is a very simple example of how to use Work Queue.
# It accepts a list of files on the command line.
# Each file is compressed with gzip and returned to the user.

from work_queue import *

import sys
import operator
import time
import ConfigParser

propertiesFileName=sys.argv[1]
#print propertiesFileName

cp= ConfigParser.RawConfigParser( )
cp.read(propertiesFileName)

port = WORK_QUEUE_DEFAULT_PORT


try:
    q = WorkQueue(port)
except:
    print "Instantiation of Work Queue failed!" 
    sys.exit(1)

print "listening on port %d..." % q.port

taskInfo = {}

basin = cp.get('workqueue-execution','basinName')
bsnDir = cp.get('workqueue-execution','basinDirectoryName')
workQueue_HOME=cp.get('workQueue-HOME','workQueue_HOME')
workQueue_TMP=cp.get('workQueue-HOME','workQueue_TMP')
trackFileName=workQueue_TMP+cp.get('trackfile-selection','outputFileName')

count = 1
f = open(trackFileName, 'r')
start_time = time.time()
for line in f:
    trackName = line.split('\t')[0]
    trackSI = line.split('\t')[1]
    trk = trackName
    rex = trackName.split('.')[0]+'.rex'
    env = trackName.split('.')[0]+'.env'

    taskInfo[count] = {}
    taskInfo[count]["track_name"] =  trackName
    taskInfo[count]["track_SI"]  = trackSI.strip()

    command = "./slosh -basin %s -bsnDir %s -trk %s -rex %s -env %s" % (basin , bsnDir , trk , rex , env)
        
    t = Task(command)

    if not t.specify_file(cp.get('workqueue-execution','sloshExecutablePath'), "slosh", WORK_QUEUE_INPUT, cache=True):
        print "specify_file() failed for /bin/slosh: check if arguments are null or remote name is an absolute path." 
        sys.exit(1) 
    if not t.specify_file(cp.get('workqueue-execution','basinDirectoryPath') , bsnDir , WORK_QUEUE_INPUT, cache=True):
        print "specify_file() failed for %s: check if arguments are null or remote name is an absolute path." % bsnDir 
        sys.exit(1)
    if not t.specify_file(cp.get('workqueue-execution','trackFilePath')+trk , trk , WORK_QUEUE_INPUT, cache=False):
        print "specify_file() failed for %s: check if arguments are null or remote name is an absolute path." % trk 
        sys.exit(1)
    if not t.specify_file(cp.get('workqueue-execution','outputPath')+'/'+rex, rex, WORK_QUEUE_OUTPUT, cache=False):
        print "specify_file() failed for %s: check if arguments are null or remote name is an absolute path." % rex 
        sys.exit(1) 
    if not t.specify_file(cp.get('workqueue-execution','outputPath')+'/'+env, env, WORK_QUEUE_OUTPUT, cache=False):
        print "specify_file() failed for %s: check if arguments are null or remote name is an absolute path." % env 
        sys.exit(1) 


    taskid = q.submit(t)

    print "submitted task (id# %d): %s" % (taskid, t.command)
    taskInfo[taskid]["start_time"] = time.time()
    count = count +1

print "waiting for tasks to complete..."

outputFilePath=workQueue_TMP+cp.get('workqueue-execution','outputFile')
outputFile=open(outputFilePath,'w')
while not q.empty():
    t = q.wait(5)
    if t:
        taskInfo[t.id]["end_time"] =  time.time()
        print "task (id# %d) complete: %s (return code %d)" % (t.id, t.command, t.return_status)
        outputFile.write(taskInfo[t.id]["track_name"].split('.')[0]+'.env')
    #task object will be garbage collected by Python automatically when it goes out of scope
end_time = time.time()

print "all tasks complete!"
print "TaskID \t Track \t Simulation Interval \t Execution Time"
for taskID in taskInfo:
    print "%d %s %s %d" % (taskID, taskInfo[taskID]["track_name"], taskInfo[taskID]["track_SI"], (taskInfo[taskID]["end_time"] - taskInfo[taskID]["start_time"])) 

print "Total time %d" % (end_time - start_time)

#work queue object will be garbage collected by Python automatically when it goes out of scope
sys.exit(0)
