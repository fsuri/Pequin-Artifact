#!/usr/bin/env bash


export PATH=$PATH:/usr/lib/postgresql/12/bin
SIZE=20G
CLUSTERID=pgdata
DATA=$(pwd)/tmp-$CLUSTERID
# echo $DATA
PGV=12
USER=postgres
pin_pg=false

display_banner() {
    local banner_text="$1"
    echo "*****************************************"
    echo "*                                       *"
    echo "          $banner_text"
    echo "*                                       *"
    echo "*****************************************"
}

setting_system() {

    echo "Try setting system configs"

    su - $USER -c "echo \"CREATE USER pequin_user WITH PASSWORD '123'\" | psql"
    #su - $USER -c "echo \"ALTER USER pequin_user WITH SUPERUSER\" | psql"

    su - $USER -c "echo \"ALTER SYSTEM SET max_connections = 250;\" | psql"
    # su - $USER -c "echo \"ALTER SYSTEM SET max_worker_processes = 16;\" | psql"
    # su - $USER -c "echo \"ALTER SYSTEM SET max_parallel_workers = 16;\" | psql"
    
    su - $USER -c "echo \"ALTER SYSTEM SET work_mem = '4GB';\" | psql"
    su - $USER -c "echo \"ALTER SYSTEM SET shared_buffers='30GB';\" | psql"
    su - $USER -c "echo \"ALTER SYSTEM SET effective_cache_size = '4GB';\" |psql"

    
    su - $USER -c "echo \"ALTER SYSTEM SET effective_io_concurrency = 8;\" |psql"
    #NOTE: If trying to use 'on' then must set some huge tables in linux
    # echo 10475 | sudo tee /proc/sys/vm/nr_hugetables
    su - $USER -c "echo \"ALTER SYSTEM SET huge_pages = 'try';\" | psql"
    su - $USER -c "echo \"ALTER SYSTEM SET max_locks_per_transaction = 1024;\" | psql"

    # su - $USER -c "echo \"ALTER SYSTEM SET synchronous_commit = 'remote_write';\" | psql"


    sudo sed -i '$a\host    all             all              0.0.0.0/0                       md5' /etc/postgresql/12/pgdata/pg_hba.conf
    d_line=$(sudo cat /etc/postgresql/12/pgdata/pg_hba.conf | grep -n  "IPv4 local" | cut -d: -f1)
    d_line=$(expr $d_line + 1)
    sudo sed -i "${d_line}d" /etc/postgresql/12/pgdata/pg_hba.conf

    cat /etc/postgresql/12/pgdata/postgresql.conf | grep -n  listen
    m_line=$(cat /etc/postgresql/12/pgdata/postgresql.conf | grep -n  listen | cut -d: -f1)
    sudo sed -i "${m_line}s/localhost/*/g" /etc/postgresql/12/pgdata/postgresql.conf
    sudo sed -i "${m_line}s/#/""/" /etc/postgresql/12/pgdata/postgresql.conf
    cat /etc/postgresql/12/pgdata/postgresql.conf | grep -n  listen

    # sudo -u postgres taskset -c 1 /usr/lib/postgresql/12/bin/postgres --config-file=/etc/postgresql/12/pgdata/postgresql.conf
    # sudo -u postgres taskset -c 1 /usr/lib/postgresql/12/bin/postgres -D /etc/postgresql/12/pgdata
    # taskset -c 1 /usr/lib/postgresql/12/bin/postgres --config-file=/etc/postgresql/12/pgdata/postgresql.conf
# 
# CONFIG FILE:   /etc/postgresql/12/pgdata/postgresql.conf


    echo "Restart Postgres"
    sudo systemctl restart postgresql
    # echo "Stopping Postgres"

    # sudo systemctl stop postgresql

    # echo "TaskSeting Postgres"

    # sudo -u postgres taskset -c 10-19 /usr/lib/postgresql/12/bin/postgres -D /etc/postgresql/12/pgdata
    # sudo -u postgres taskset -c 10-19 /usr/lib/postgresql/12/bin/postgres -D /etc/postgresql/12/pgdata

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
    # su - $USER -c "echo \"ALTER DATABASE $dbname SET lock_timeout = 100 ;\" | psql -d $dbname"
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
    exit 1
fi


### removing cluster from the system
if [ "$drop_postgres_cluster" = true ] ; then
    display_banner "Dropping Postgres Cluster" 

    # If postgres is not installed, the following will produce error, that is ok
    sudo pg_dropcluster --stop $PGV $CLUSTERID
    sudo umount $DATA
    #sudo umount /users/fs435/tmp-pgdata
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
    sudo bash -c 'echo "ssl-cert:x:115" >> /etc/group'
    sudo apt update
    sudo apt install postgresql
    sudo sed -i '$ d' /etc/group
    sudo apt install postgresql-common
    
    # Creating postgres user to use the postgres service
    sudo useradd -m postgres || true
    sudo passwd -d postgres

    gid=$(cat /etc/group | grep postgres | cut -d: -f3)
    sudo usermod -g $gid $USER

    # Removing the main cluster, if it wascreated during the installation (it prevents from connecting to our designated one later)
    sudo pg_dropcluster --stop $PGV main
    sudo systemctl daemon-reload
fi



display_banner "Initializing Postgres Cluster" 


# Verifying that no clusters exist at this point
output="$(pg_lsclusters -h)"
if [[ -n $output ]] ; then
    echo "The following clusters exist, continue manually:"
    echo $output
else
    echo "No cluster exists, moving on to creating pgdata cluster"
    # exit -1
    # creating a PostgreSQL cluster in a ramdisk (in order to run experiments that are not bias by slow disk memory)
    mkdir -p $DATA/db $DATA/log || true

    ### Comment this to cancel mounting
    sudo mount -t tmpfs -o size=$SIZE,nr_inodes=10k,mode=0777 tmpfs $DATA

    # sudo systemctl stop postgresql

    # sudo pg_createcluster -u $USER -d $DATA/db $PGV $CLUSTERID -l $DATA/log --start-conf=manual -s $DATA/socket -p 5432
    sudo pg_createcluster -u $USER -d $DATA/db $PGV $CLUSTERID --start-conf=manual -s $DATA/socket -p 5432
    # sudo -u postgres taskset -c 10-19 pg_createcluster -u $USER -d $DATA/db $PGV $CLUSTERID -l $DATA/log --start-conf=manual -s $DATA/socket -p 5432


    # exit -1


    sudo rsync -avz $DATA/db/ $DATA/dbinit/
    sudo sed -i '89s/^/local   all             all                                     trust\n/' /etc/postgresql/12/pgdata/pg_hba.conf

    # echo "Here1"
    # sudo systemctl stop postgresql
    # sudo -u postgres taskset -c 10-19 /usr/lib/postgresql/12/bin/postgres -D /etc/postgresql/12/pgdata
    # echo "Here2"

    # Start the PostgreSQL cluster
    sudo pg_ctlcluster $PGV $CLUSTERID start

    # Setting the system
    setting_system
  


    if [ "$pin_pg" = true ] ; then

        PIDS=$(pgrep -u postgres) 
        for PID in $PIDS; do
            sudo taskset -p -p 0xFFC00 $PID
        done
        
    fi

    # Setting the databases
    for i in $(seq 1 $db_num);
    do
	setting_db db$i
    done

fi

# sudo cp /usr/local/etc/postgresql_copy.conf /etc/postgresql/12/pgdata/postgresql.conf
# sudo cp /usr/local/etc/pg_hba_copy.conf /etc/postgresql/12/pgdata/pg_hba.conf


#state where to run this scrit from
# "df" command shows the list of mounted devices
# use umount "folder" n order to unmount. i'm not sure why but sometimes you would have to do it more than once
# only after you unmounted you are able to delete the folder

# mount: /home/sc3348/Pesto/Pequin-Artifact/tmp-pgdata: mount point does not exist.






