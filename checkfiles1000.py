#!/usr/bin/env python/
'''This file reads through the output files and checks whether any of the outputfiles in the sequence is missing. If the file is missing, the index of the missing files is returned to a text file.
Bex Dunn May 2017'''

import os

#Variable N is the number of files expected.
N=6743

DirContents = os.listdir( '/g/data/r78/rjd547/groundwater_activities/Stuart_Cor/Stu_5k_raijin/TCI_1000/')
filenumber = []

#make a list of the runs we have:
for filename in DirContents:
    if '.nc' in filename:
        newname = filename.split('_')
        newname = str(newname[3])
        #print(newname)    
        filenumber.append(int(newname))

SetofRuns = set(filenumber)
#print(SetofRuns)
SetofWantedRuns =set(range(0,N,1))
NotThere = SetofRuns.symmetric_difference(SetofWantedRuns)
#The index of the run is one more than the index of the file, as the python script 
NotThereIx = list(NotThere)
NotThereIx = [x+1 for x in NotThereIx]
print("there are ",str(len(NotThereIx)),"missing runs and ","the missing runs are: ",NotThereIx)

f=open('MissingRuns_1000_1.txt','w')
for run in NotThereIx:
    f.write('%s\n' %run)
f.close()	
