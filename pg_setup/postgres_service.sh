#!/usr/bin/env bash


export PATH=$PATH:/usr/lib/postgresql/12/bin
SIZE=1024M
CLUSTERID=pgdata
DATA=$(pwd)/tmp-$CLUSTERID
# echo $DATA
PGV=12
USER=postgres


display_banner() {
    local banner_text="$1"
    echo "*****************************************"
    echo "*                                       *"
    echo "          $banner_text"
    echo "*                                       *"
    echo "*****************************************"
}


setting_db() {
    local dbname="$1"
    echo "Setting $dbname:"
    su - $USER -c "echo \"SELECT 'CREATE DATABASE $dbname' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$dbname')\gexec\" | psql"
    su - $USER -c "echo \"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pequin_user\" | psql -d $dbname"
    su - $USER -c "echo \"alter default privileges in schema public grant all on tables to pequin_user; alter default privileges in schema public grant all on sequences to pequin_user;\" | psql -d $dbname"
    su - $USER -c "echo \"GRANT pg_read_server_files TO pequin_user;\" | psql -d $dbname"
    su - $USER -c "echo \"ALTER DATABASE $dbname SET DEFAULT_TRANSACTION_ISOLATION TO SERIALIZABLE ;\" | psql -d $dbname"
    su - $USER -c "echo \"ALTER DATABASE $dbname SET ENABLE_MERGEJOIN TO FALSE ;\" | psql -d $dbname"
    su - $USER -c "echo \"ALTER DATABASE $dbname SET ENABLE_HASHJOIN TO FALSE ;\" | psql -d $dbname"
    su - $USER -c "echo \"ALTER DATABASE $dbname SET ENABLE_NESTLOOP TO TRUE ;\" | psql -d $dbname"
    su - $USER -c "echo \"ALTER DATABASE $dbname SET lock_timeout = 100 ;\" | psql -d $dbname"
}


unistall_flag=false
drop_postgres_cluster=false
db_num=1
reset_cluster=false

# Parse command-line options
# while test $# != 0; do
#     case "$1" in
while getopts 'urscn:v' flag; do
    case "${flag}" in
    u) 
        read -r -p "Are you sure you want to uninstall postgres? [y/N] " response
        case "$response" in
            [yY][eE][sS]|[yY]) 
                unistall_flag=true
                ;;
            *)
                exit 1
                ;;
        esac
        ;;

    r) 
        drop_postgres_cluster=true
        echo "Dropping existing postgres clusters"
        ;;
    s) 
        reset_cluster=true
        echo "Trying to reset the cluster"
        ;;
    
    c)   # Cloud lab option: if things still exist: just reset, and otherwise create db
        echo "Preparing cloudlab for exp."
        output="$(pg_lsclusters -h)"
        if [[ -n $output ]] ; then
            echo "A cluster already exists, just reset it"
            reset_cluster=true
        else
            echo "No cluster exists, moving on to creating pgdata cluster with 1 db"
        fi
        ;;

    n)
        db_num=$2
        echo "Setting up $db_num databases"
        ;;

    *)  echo "Invalid option: -$1"
        exit 1
        ;;
    esac
    shift
done



### uninstalling postgres
if [ "$unistall_flag" = true ] ; then
    display_banner "Uninstalling Postgres" 
    sudo apt-get remove --purge postgresql\* postgres\*
    sudo apt-get autoremove
    sudo apt-get autoclean
    sudo apt update
    exit 1
fi


### removing cluster from the system
if [ "$drop_postgres_cluster" = true ] ; then
    display_banner "Dropping Postgres Cluster" 

    # If postgres is not installed, the following will produce error, that is ok
    sudo pg_dropcluster --stop $PGV $CLUSTERID
    sudo umount $DATA
    output="$(df | grep $DATA)"
    if [[ -n $output ]] ; then
        echo "The unmounting operation did not succeed, continue manually"
    else
        sudo rm -rf $DATA
    fi
    exit 1
fi

### reseting the cluster by deleting and recreating the public schema
if [ "$reset_cluster" = true ] ; then
    display_banner "Reseting Postgres Cluster" 

    output=$(su - $USER -c "echo \"SELECT datname FROM pg_database ;\" | psql")

    # Remove all dbs with name db*
    echo "$output" | grep -oE '\bdb\w*' | while read -r dbname; do

        echo "Dropping database $dbname"
        su - $USER -c "echo \" DROP DATABASE IF EXISTS \"$dbname\";\" | psql"

        setting_db $dbname

    done
    exit 1
fi


### If we got here- we first verify that postgres is installed, then create our cluster

### Install postgres, if not already installed
if dpkg -l | grep postgresql -q ; then
    echo "Postgres is installed already..."
else
    display_banner "Postgres is not yet installed, installing it now..."
    sudo echo "ssl-cert:x:115" >> /etc/group
    sudo apt install postgresql
    sudo sed -i '$ d' /etc/group
    sudo apt install postgresql-common

    # Removing the main cluster, if it wascreated during the installation (it prevents from connecting to our designated one later)
    sudo pg_dropcluster --stop $PGV main
    sudo systemctl daemon-reload
fi



display_banner "Initializing Postgres Cluster" 

# Creating postgres user to use the postgres service
sudo useradd -m $USER || true
sudo passwd -d $USER


# Verifying that no clusters exist at this point
output="$(pg_lsclusters -h)"
if [[ -n $output ]] ; then
    echo "The following clusters exist, continue manually:"
    echo $output
else
    echo "No cluster exists, moving on to creating pgdata cluster"
    # creating a PostgreSQL cluster in a ramdisk (in order to run experiments that are not bias by slow disk memory)
    sudo mkdir -p $DATA/db $DATA/log || true
    sudo mount -t tmpfs -o size=$SIZE,nr_inodes=10k,mode=0777 tmpfs $DATA
    

    sudo pg_createcluster -u $USER -d $DATA/db $PGV $CLUSTERID -l $DATA/log --start-conf=manual -s $DATA/socket -p 5432
    sudo rsync -avz $DATA/db/ $DATA/dbinit/
    sudo sed -i '89s/^/local   all             all                                     trust\n/' /etc/postgresql/12/pgdata/pg_hba.conf

    # Start the PostgreSQL cluster
    sudo pg_ctlcluster $PGV $CLUSTERID start

    su - $USER -c "echo \"CREATE USER pequin_user WITH PASSWORD '123';\" | psql"


    su - $USER -c "echo \"SHOW max_connections;\" | psql"
    su - $USER -c "echo \"SHOW max_worker_processes;\" | psql"
    su - $USER -c "echo \"SHOW max_parallel_workers;\" | psql"
    su - $USER -c "echo \"ALTER SYSTEM SET max_connections = 250;\" | psql"
    # su - $USER -c "echo \"ALTER SYSTEM SET max_worker_processes = 16;\" | psql"
    # su - $USER -c "echo \"ALTER SYSTEM SET max_parallel_workers = 16;\" | psql"
    su - $USER -c "echo \"SELECT pg_reload_conf();\" | psql"
    sudo systemctl restart postgresql
    su - $USER -c "echo \"SHOW max_connections;\" | psql"
    # su - $USER -c "echo \"SHOW max_worker_processes;\" | psql"
    # su - $USER -c "echo \"SHOW max_parallel_workers;\" | psql"

    # Setting the databases
    for i in $(seq 1 $db_num);
    do
        setting_db db$i
    done

fi


#state where to run this scrit from
# "df" command shows the list of mounted devices
# use umount "folder" n order to unmount. i'm not sure why but sometimes you would have to do it more than once
# only after you unmounted you are able to delete the folder

# mount: /home/sc3348/Pesto/Pequin-Artifact/tmp-pgdata: mount point does not exist.






