from __future__ import absolute_import, division, print_function, unicode_literals

"""
Three folders, with *mbps and *ratio files
Output:

Individual files: {Filename}_{DictSize}_{CompressionLevel}_ratio
    Rows: ZSTD_old, ZSTD_nodict, ZSTD_newpath
    Cols: Filesize
    Entries: Compression Ratio

Individual files: {Filename}_{DictSize}_{CompressionLevel}_mbps
    Rows: ZSTD_old_mean, ZSTD_old_CI, ZSTD_nodict_mean, ZSTD_nodict_CI, ZSTD_newpath_mean, ZSTD_newpath_CI, ZSTD_old_change, ZSTD_nodict_change
    Cols: Filesize
    Entries: Compression Speed stats for each

RatioAggregateConclusionsVsOld{Filename}:
    Rows: DictSize
    Cols: Compression level
    Entries: The filesize at which ZSTD_newpath_ratio > ZSTD_old_ratio
    Implementation: nested dict: {filename:{dictsize,clevel:filesizecutoff}}

RatioAggregateConclusionsVsNoDict{Filename}:
    Rows: DictSize
    Cols: Compression level
    Entries: The filesize at which ZSTD_newpath_ratio > ZSTD_old_ratio

MbpsAggregateConclusionsVsOld{Filename}:
    Rows: DictSize
    Cols: Compression level
    Entries: Filesize at which ZSTD_newpath_mbps is confidently > ZSTD_old_mbps, 0 if never

MbpsAggregateConclusionsVsNoDict{Filename}:
    Rows: DictSize
    Cols: Compression level
    Entries: Filesize at which ZSTD_newpath_mbps is confidently > ZSTD_old_mbps, 0 if never

"""


import shutil
import os
import subprocess
import collections
import math

OldRatioFilesizeThresholdMap = {}
NoDictRatioFilesizeThresholdMap = {}

newlistpath = "/Users/senhuang96/bench/results/zstdnew/"
oldlistpath = "/Users/senhuang96/bench/results/zstdold/"
nodictlistpath = "/Users/senhuang96/bench/results/zstdoldnodict/"

newlist = os.listdir(newlistpath)
newlist.sort()
oldlist = os.listdir(oldlistpath)
oldlist.sort()
oldlistnodict = os.listdir(nodictlistpath)
oldlistnodict.sort()
savepath = "/Users/senhuang96/bench/results/zstdaggresults/"
savepath2 = "/Users/senhuang96/bench/results/zstdfinalres/"

# Bring all the indidividual results into one single ratio file per input file
# Also compute the dictionary from {dictsize, clevel -> filebreakpoint}
for (newResFile, oldResFile, oldNoDictResFile) in zip(newlist, oldlist, oldlistnodict):
    if "ratio" not in newResFile:
        continue
    tokenizedFilename = newResFile.split('_')

    filename = tokenizedFilename[0]
    dictSize = tokenizedFilename[1]
    compLevel = tokenizedFilename[2]
    keyPair = (filename, dictSize, compLevel)

    fNewResFile = open(newlistpath + newResFile)
    fOldResFile = open(oldlistpath + oldResFile)
    fOldNoDictResFile = open(nodictlistpath + oldNoDictResFile)

    linesNewRes = fNewResFile.readlines()
    linesOldRes = fOldResFile.readlines()
    lineNoDictRes = fOldNoDictResFile.readlines()

    alreadyDeterminedOldBreakpoint = False
    alreadyDeterminedNoDictBreakpoint = False

    f = open(savepath + filename + "_" + dictSize + "_" + compLevel + "_" + "ratio", "w")
    f.write("Filesize,ZSTD_old,ZSTD_nodict,ZSTD_new\n")
    f.close()

    for (newRes, oldRes, noDictRes) in zip(linesNewRes, linesOldRes, lineNoDictRes):

        newResTup = [x.strip() for x in newRes.split(',')]
        oldResTup = [x.strip() for x in oldRes.split(',')]
        noDictResTup = [x.strip() for x in noDictRes.split(',')]

        fileSize = newResTup[0]

        newRatio = newResTup[1]
        oldRatio = oldResTup[1]
        noDictRatio = noDictResTup[1]

        newRow = fileSize + ',' + oldRatio + ',' + noDictRatio + ',' + newRatio + '\n'
        f = open(savepath + filename + "_" + dictSize + "_" + compLevel + "_" + "ratio", "a")
        f.write(newRow)
        f.close()
        if not alreadyDeterminedOldBreakpoint:
            if float(newRatio) > float(oldRatio):
                OldRatioFilesizeThresholdMap[keyPair] = fileSize
                alreadyDeterminedOldBreakpoint = True
        if not alreadyDeterminedNoDictBreakpoint:
            if float(newRatio) > float(noDictRatio):
                NoDictRatioFilesizeThresholdMap[keyPair] = fileSize
                alreadyDeterminedNoDictBreakpoint = True

    # new path was never better
    if alreadyDeterminedOldBreakpoint == False:
        OldRatioFilesizeThresholdMap[keyPair] = str(-1)

NestedOldRatioFilesizeThresholdMap = {}
NestedNoDictRatioFilesizeThresholdMap = {}
# Transform to nested dicts
for k, v in OldRatioFilesizeThresholdMap.items():
    NestedOldRatioFilesizeThresholdMap[k[0]] = {}
for k, v in OldRatioFilesizeThresholdMap.items():
    NestedOldRatioFilesizeThresholdMap[k[0]][(int(k[1]), int(k[2]))] = int(v)
for k, v in NoDictRatioFilesizeThresholdMap.items():
    NestedNoDictRatioFilesizeThresholdMap[k[0]] = {}
for k, v in NoDictRatioFilesizeThresholdMap.items():
    NestedNoDictRatioFilesizeThresholdMap[k[0]][(int(k[1]), int(k[2]))] = int(v)

# Generate aggregate ratio filesize breakpoint table
for filename, innerdict in NestedOldRatioFilesizeThresholdMap.items():
    innerdictSorted = sorted(innerdict.items())
    # {(2,4): 1, (1,3): 2, (1,2): 3, (3,1): 4} -> [((1, 2), 3), ((1, 3), 2), ((2, 4), 1), ((3, 1), 4)] 
    prevDictSize = -1
    resultFilename = savepath2 + filename + "_old_ratio"
    f = open(resultFilename, "a+")
    for each in innerdictSorted:
        dictsize = each[0][0]
        complevel = each[0][1]
        filesize = each[1]
        if prevDictSize != -1 and prevDictSize != dictsize:
            f.write("\n")
        f.write(str(filesize) + ",")
        prevDictSize = dictsize
    f.close()

for filename, innerdict in NestedNoDictRatioFilesizeThresholdMap.items():
    innerdictSorted = sorted(innerdict.items())
    # {(2,4): 1, (1,3): 2, (1,2): 3, (3,1): 4} -> [((1, 2), 3), ((1, 3), 2), ((2, 4), 1), ((3, 1), 4)] 
    prevDictSize = -1
    resultFilename = savepath2 + filename + "_nodict_ratio"
    f = open(resultFilename, "a+")
    for each in innerdictSorted:
        dictsize = each[0][0]
        complevel = each[0][1]
        filesize = each[1]
        if prevDictSize != -1 and prevDictSize != dictsize:
            f.write("\n")
        f.write(str(filesize) + ",")
        prevDictSize = dictsize
    f.close()

"""
Mbps stuff

"""

NUM_TRIALS = 5 # change this based on how many trials we ran in lower level benchmarks

def calculateDiffMeansCi(stdev1, stdev2, trials):
    z = 1.96
    term = math.sqrt((stdev1*stdev1)/trials + (stdev2*stdev2)/trials)
    return z * term

OldMbpsFilesizeThresholdMap = {}
NoDictMbpsFilesizeThresholdMap = {}

for (newResFile, oldResFile, oldNoDictResFile) in zip(newlist, oldlist, oldlistnodict):
    if "mbps" not in newResFile:
        continue
    tokenizedFilename = newResFile.split('_')

    filename = tokenizedFilename[0]
    dictSize = tokenizedFilename[1]
    compLevel = tokenizedFilename[2]
    keyPair = (filename, dictSize, compLevel)

    fNewResFile = open(newlistpath + newResFile)
    fOldResFile = open(oldlistpath + oldResFile)
    fOldNoDictResFile = open(nodictlistpath + oldNoDictResFile)

    linesNewRes = fNewResFile.readlines()
    linesOldRes = fOldResFile.readlines()
    lineNoDictRes = fOldNoDictResFile.readlines()

    numSigFasterThanOld = 0
    numSigSlowerThanOld = 0
    numAboutSameAsOld = 0
    numSigFasterThanNodict = 0
    numSigSlowerThanNodict = 0
    numAboutSameAsNodict = 0

    f = open(savepath + filename + "_" + dictSize + "_" + compLevel + "_" + "mbps", "w+")
    f.write("Filesize,ZSTD_old_mean,ZSTD_old_ci,ZSTD_nodict_mean,ZSTD_nodict_ci,ZSTD_new_mean,ZSTD_new_ci,ZSTD_new_old_change,ZSTD_new_old_change_ci,ZSTD_new_nodict_change,ZSTD_new_nodict_change_ci\n")
    f.close()

    for (newRes, oldRes, noDictRes) in zip(linesNewRes, linesOldRes, lineNoDictRes):

        newResTup = [x.strip() for x in newRes.split(',')]
        oldResTup = [x.strip() for x in oldRes.split(',')]
        noDictResTup = [x.strip() for x in noDictRes.split(',')]

        fileSize = newResTup[0]
        if (oldResTup[0] != fileSize or noDictResTup[0] != fileSize):
            raise Exception('why are the filesizes not the same, they should be')

        oldMean = oldResTup[1]
        oldCi = oldResTup[2]
        oldStd = oldResTup[3]
        nodictMean = noDictResTup[1]
        nodictCi = noDictResTup[2]
        nodictStd = noDictResTup[3]
        newMean = newResTup[1]
        newCi = newResTup[2]
        newStd = newResTup[3]

        newOldChange = float(newMean)-float(oldMean)
        newOldCi = calculateDiffsMeansCi(float(newStd), float(oldStd), NUM_TRIALS)
        newNodictChange = float(newMean)-float(nodictMean)
        newNodictCi = calculateDiffMeansCi(float(newStd), float(oldStd), NUM_TRIALS)

        newRow = fileSize + ',' + oldMean + ',' + oldCi + ',' + nodictMean + ',' + nodictCi  + ',' 
        + newMean  + ',' + newCi + ',' + newOldChange  + ',' + newOldCi  + ',' + newNodictChange  + ',' + newNodictCi + '\n'

        f = open(savepath + filename + "_" + dictSize + "_" + compLevel + "_" + "mbps", "a")
        f.write(newRow)
        f.close()

        if newOldChange > 0 and newOldChange - newOldCi > 0:
            numSigFasterThanOld += 1
        elif newOldChange < 0 and newOldChange + newOldCi < 0:
            numSigSlowerThanOld += 1
        else:
            numAboutSameAsOld += 1
        
        if newNodictChange > 0 and newNodictChange - newNodictCi > 0:
            numSigFasterThanNodict += 1
        elif newNodictChange < 0 and newNodictChange + newNodictCi < 0:
            numSigSlowerThanNodict += 1
        else:
            numAboutSameAsNodict += 1

    f = open(savepath + filename + "_" + dictSize + "_" + compLevel + "_" + "speedRoughComp", "w+")
    f.write("sig_faster_old,sig_slower_old,same_old,sig_faster_nodict,sig_slower_nodict,same_nodict\n")
    f.write(str(numSigFasterThanOld) + "," + str(numSigSlowerThanOld) + "," +
    str(numAboutSameAsOld) + "," + str(numSigFasterThanNodict) + "," + str(numSigSlowerThanNodict) + "," + str(numAboutSameAsNodict))
    f.close()
