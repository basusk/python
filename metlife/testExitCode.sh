#!/bin/bash
#########################################################################################
########Author: Saikat Basu, Senior Solution Architect, Kyligence Inc.
########Version 1: Simple wrapper script to run python program #########################
########Version 1: And to capture exit codes from python program #######################
########Version 1: Date : 19th July 2023                         #######################
#########################################################################################

python -u parallel_job_submission.py > log_u.txt 2>&1

if [ $? != 0 ];
then
	echo "error"

else 
	echo "success"
fi
