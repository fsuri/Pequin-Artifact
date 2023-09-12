#!/usr/bin/env bash

#sudo apt install postgresql
sudo useradd -m postgres || true
sudo passwd -d postgres

export PATH=$PATH:/usr/lib/postgresql/12/bin
SIZE=1024M
CLUSTERID=pgdata
DATA=$(pwd)/tmp-$CLUSTERID
echo $DATA
PGV=12
USER=postgres

mkdir -p $DATA/db $DATA/log || true
sudo mount -t tmpfs -o size=$SIZE,nr_inodes=10k,mode=0777 tmpfs $DATA
sudo pg_createcluster -u $USER -d $DATA/db $PGV $CLUSTERID \
                -l $DATA/log --start-conf=manual -s $DATA/socket
sudo rsync -avz $DATA/db/ $DATA/dbinit/
sudo sed -i '89s/^/local   all             all                                     trust\n/' /etc/postgresql/12/pgdata/pg_hba.conf
sudo pg_ctlcluster $PGV $CLUSTERID start


# The below code can be put into a loop and automated it is decided that more databases are needed
su - $USER -c "echo \"SELECT 'CREATE DATABASE db1' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'db1')\gexec\" | psql"
su - $USER -c "echo \"CREATE USER pequin_user\" | psql"
su - $USER -c "echo \"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pequin_user\" | psql -d db1"
su - $USER -c "echo \"alter default privileges in schema public grant all on tables to pequin_user; alter default privileges in schema public grant all on sequences to pequin_user;\" | psql -d db1"



#if [ -z "${1//[0-9]}" ]
#then
#    if [ -n "$1" ]
#    then

#    fi
#else
#    echo "Please include an integer argument for the number of databases needed"
#fi

