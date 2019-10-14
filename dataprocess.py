from __future__ import absolute_import, division, print_function, unicode_literals

"""
Pseudocode:
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

OldRatioFilesizeThresholdMap = {}
NoDictRatioFilesizeThresholdMap = {}
OldMbpsFilesizeThresholdMap = {}
NoDictMbpsFilesizeThresholdMap = {}

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
        alreadyDeterminedOldBreakpoint = True

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

    alreadyDeterminedOldBreakpoint = False
    alreadyDeterminedNoDictBreakpoint = False

    f = open(savepath + filename + "_" + dictSize + "_" + compLevel + "_" + "ratio", "w")
    f.write("Filesize,ZSTD_old_mean,ZSTD_old_ci,ZSTD_nodict_mean,ZSTD_nodict_ci,ZSTD_new_mean,ZSTD_new_ci\n")
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
        alreadyDeterminedOldBreakpoint = True

    
"""
# Bring all the indidividual results into one single ratio file per input file
# Also compute the dictionary from {dictsize, clevel -> filebreakpoint}
for (newResFile, oldResFile, oldNoDictResFile) in zip(newlist, oldlist, oldlistnodict):
    if "ratio" not in newResFile:
        continue
    tokenizedFilename = newResFile.split('_')

    filename = tokenizedFilename[0]
    dictSize = tokenizedFilename[1]
    compLevel = tokenizedFilename[2]

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
                keyPair = (filename, dictSize, compLevel)
                OldRatioFilesizeThresholdMap[keyPair] = fileSize
                alreadyDeterminedOldBreakpoint = True
        if not alreadyDeterminedNoDictBreakpoint:
            if float(newRatio) > float(noDictRatio):
                keyPair = (filename, dictSize, compLevel)
                NoDictRatioFilesizeThresholdMap[keyPair] = fileSize
                alreadyDeterminedNoDictBreakpoint = True
"""