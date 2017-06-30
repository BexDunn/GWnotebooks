#!/bin/bash
#PBS -P r78
#PBS -q normal
#PBS -l walltime=01:00:00
#PBS -l mem=1150GB   
#PBS -l ncpus=144
#PBS -l wd
#PBS -N Stwet400_nbt576
 
FIRST=/g/data/r78/rjd547/groundwater_activities/scripts/Stuart_Cor_400_nbart_run/Run_python_TCI.sh
node_count=$PBS_NCPUS
N=576
for node in $(seq 1 $node_count); do
 echo $(($node + $N))
 pbsdsh -n $((node)) -- bash -l -c "$FIRST $(($node + $N))" &
done
 
wait

depend=afterany:$FIRST
N=720
for node in $(seq 1 $node_count); do
 echo $(($node + $N))
 pbsdsh -n $((node)) -- bash -l -c "$FIRST $(($node + $N))" &
done

wait

depend=afterany:$FIRST
N=864
for node in $(seq 1 $node_count); do
 echo $(($node + $N))
 pbsdsh -n $((node)) -- bash -l -c "$FIRST $(($node + $N))" &
done

wait

depend=afterany:$FIRST
N=1008
for node in $(seq 1 $node_count); do
 echo $(($node + $N))
 pbsdsh -n $((node)) -- bash -l -c "$FIRST $(($node + $N))" &
done

wait


