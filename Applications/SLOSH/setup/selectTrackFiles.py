#!/usr/bin/env python

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This program is a very simple example of how to use Work Queue.
# It accepts a list of files on the command line.
# Each file is compressed with gzip and returned to the user.

import sys
import operator
import ConfigParser

propertiesFileName=sys.argv[1]
print propertiesFileName

cp= ConfigParser.RawConfigParser( )
cp.read(propertiesFileName)
##print cp.get('trackfile-selection','noOfTrackfile')

##fileName = raw_input('Enter path of the file containing trackfile name and corresponding simulation interval?\n')
##trackFileCol = int(raw_input('Enter the column number of trackfiles?\n'))
##simulationIntervalCol = int(raw_input('Enter the column number of simulation interval?\n'))
##colSeparator = raw_input('Enter the seperator between the columns?\n')
workQueue_HOME=cp.get('workQueue-HOME','workQueue_HOME')
workQueue_TMP=cp.get('workQueue-HOME','workQueue_TMP')
fileName=cp.get('trackfile-selection','trackFileSM')
print(fileName)
trackFileCol = int(cp.get('trackfile-selection','trackFileColumNumber'))
simulationIntervalCol = int(cp.get('trackfile-selection','simulationIntervalColumn'))
#colSeparator = cp.get('trackfile-selection','columnSeparator')

trackFileDict = {}
trackFile = open(fileName,'r')

for line in trackFile:
    line = line.strip().split()
    trackFileDict[line[trackFileCol-1]] = line[simulationIntervalCol-1]

trackFile.close()

sorted_x = sorted(trackFileDict.iteritems(), key=operator.itemgetter(1))

print 'Trackfiles sorted in descending order'

##numberOfTrackFiles = int(raw_input('Enter the number of trackfiles for which simulation needs to be run?\n'))
##selectionCriteria = raw_input('Enter the selection criteria for choosing trackfiles?\n 1 for First '+str(numberOfTrackFiles)+'\n 2 for last '+str(numberOfTrackFiles)+' \n 3 for random '+str(numberOfTrackFiles)+'\n')

numberOfTrackFiles = int(cp.get('trackfile-selection','noOfTrackfile'))
selectionCriteria = int(cp.get('trackfile-selection','selectionCriteria'))
outputFileName=workQueue_TMP+cp.get('trackfile-selection','outputFileName')
##print numberOfTrackFiles
##print selectionCriteria

sizeOfEachPartition = len(trackFileDict)/numberOfTrackFiles

outputFile = open(outputFileName, 'w')

for i in reversed(range(0, numberOfTrackFiles)):
    outputFile.write(sorted_x[sizeOfEachPartition*i+0][0] + '\t ' + sorted_x[sizeOfEachPartition*i+0][1]+'\n')

outputFile.close()




##
##basin = 'hmi3'
##bsnDir = 'dta'
##count = 1
##start_time = time.time()
##for i in reversed(range(0, 20)):
##    trackName = sorted_x[sizeOfEachPartition*i+0][0]
##    trackSI = sorted_x[sizeOfEachPartition*i+0][1]
##    trk = trackName
##    rex = trackName.split('.')[0]+'.rex'
##    env = trackName.split('.')[0]+'.env'
##
##    taskInfo[count] = {}
##    taskInfo[count]["track_name"] =  trackName
##    taskInfo[count]["track_SI"]  = trackSI
##
##    command = "./slosh -basin %s -bsnDir %s -trk %s -rex %s -env %s" % (basin , bsnDir , trk , rex , env)
##        
##    t = Task(command)
##
##    if not t.specify_file("SLOSH/slosh/bin/slosh", "slosh", WORK_QUEUE_INPUT, cache=True):
##        print "specify_file() failed for /bin/slosh: check if arguments are null or remote name is an absolute path." 
##        sys.exit(1) 
##    if not t.specify_file("SLOSH/slosh/dta" , bsnDir , WORK_QUEUE_INPUT, cache=True):
##        print "specify_file() failed for %s: check if arguments are null or remote name is an absolute path." % bsnDir 
##        sys.exit(1)
##    if not t.specify_file("SLOSH/track-files/meow_trkfiles_hmi/"+trk , trk , WORK_QUEUE_INPUT, cache=False):
##        print "specify_file() failed for %s: check if arguments are null or remote name is an absolute path." % trk 
##        sys.exit(1)
##    if not t.specify_file("output/"+rex, rex, WORK_QUEUE_OUTPUT, cache=False):
##        print "specify_file() failed for %s: check if arguments are null or remote name is an absolute path." % rex 
##        sys.exit(1) 
##    if not t.specify_file("output/"+env, env, WORK_QUEUE_OUTPUT, cache=False):
##        print "specify_file() failed for %s: check if arguments are null or remote name is an absolute path." % env 
##        sys.exit(1) 
##
##
##    taskid = q.submit(t)
##
##    print "submitted task (id# %d): %s" % (taskid, t.command)
##    taskInfo[taskid]["start_time"] = time.time()
##    count = count +1
##
##print "waiting for tasks to complete..."
##
##while not q.empty():
##    t = q.wait(5)
##    if t:
##        taskInfo[t.id]["end_time"] =  time.time()
##        print "task (id# %d) complete: %s (return code %d)" % (t.id, t.command, t.return_status)
##    #task object will be garbage collected by Python automatically when it goes out of scope
##end_time = time.time()
##
##print "all tasks complete!"
##print "TaskID \t Track \t Simulation Interval \t Execution Time"
##for taskID in taskInfo:
##    print "%d \t %s \t %s \t %d" % (taskID, taskInfo[taskID]["track_name"], taskInfo[taskID]["track_SI"], (taskInfo[taskID]["end_time"] - taskInfo[taskID]["start_time"])) 
##
##print "Total time %d" % (end_time - start_time)
##
###work queue object will be garbage collected by Python automatically when it goes out of scope
##sys.exit(0)
