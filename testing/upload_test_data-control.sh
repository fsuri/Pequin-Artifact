#!/bin/bash

##use this if I want to upload things onto the machines..
##scp  -r file bcyl2@${machine}.indicus.morty-pg0.utah.cloudlab.us:/users/bcyl2/
## ssh ... sudo mv .. ...


CLOUDLAB_USER="bcyl2"
EXPERIMENT_NAME="pequin"
PROJECT_NAME="crdbdriver"

while getopts u:e:p: option; do
case "${option}" in
u) CLOUDLAB_USER=${OPTARG};;
e) EXPERIMENT_NAME=${OPTARG};;
p) PROJECT_NAME=${OPTARG};;
esac;
done

## declare an array variable
declare -a arr_servers=( "us-east-1-0" "us-east-1-1" "us-east-1-2"
						"eu-west-1-0" "eu-west-1-1" "eu-west-1-2"
						"ap-northeast-1-0" "ap-northeast-1-1" "ap-northeast-1-2"
			  		   )

## now loop through the above array
for host in "${arr_servers[@]}"
do
   echo "uploading binaries to $host"
   #ssh bcyl2@$host.indicus.morty-pg0.utah.cloudlab.us "sudo rm -rf /mnt/extra/experiments/*"
   rsync -v -r -e ssh testing/test-tables-schema.json $CLOUDLAB_USER@$host.$EXPERIMENT_NAME.$PROJECT_NAME-pg0.utah.cloudlab.us:/users/bcyl2/indicus/bin/
   rsync -v -r -e ssh testing/test-1k.csv $CLOUDLAB_USER@$host.$EXPERIMENT_NAME.$PROJECT_NAME-pg0.utah.cloudlab.us:/users/bcyl2/indicus/bin/
done



