#!/bin/bash

module purge

module load pbs

module use /g/data/v10/public/modules/modulefiles/

module load otps

module load agdc-py3-prod/1.4.1

export PYTHONPATH="/g/data/r78/rjd547/groundwater_activities/scripts/WYDY/agdc_statistics:${PYTHONPATH}"
