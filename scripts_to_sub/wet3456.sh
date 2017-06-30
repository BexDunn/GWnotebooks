#!/bin/bash
#PBS -P r78
#PBS -q normal
#PBS -l walltime=01:00:00
#PBS -l mem=1150GB   
#PBS -l ncpus=144
#PBS -l wd
#PBS -N Stwet400_nbt3456
 
FIRST=/g/data/r78/rjd547/groundwater_activities/scripts/Stuart_Cor_400_nbart_run/Run_python_TCI.sh
node_count=$PBS_NCPUS
N=2880
for node in $(seq 1 $node_count); do
 echo $(($node + $N))
 pbsdsh -n $((node)) -- bash -l -c "$FIRST $(($node + $N))" &
done
 
wait

depend=afterany:$FIRST
N=3024
for node in $(seq 1 $node_count); do
 echo $(($node + $N))
 pbsdsh -n $((node)) -- bash -l -c "$FIRST $(($node + $N))" &
done

wait

depend=afterany:$FIRST
N=3168
for node in $(seq 1 $node_count); do
 echo $(($node + $N))
 pbsdsh -n $((node)) -- bash -l -c "$FIRST $(($node + $N))" &
done

wait

depend=afterany:$FIRST
N=3312
for node in $(seq 1 $node_count); do
 echo $(($node + $N))
 pbsdsh -n $((node)) -- bash -l -c "$FIRST $(($node + $N))" &
done

wait


