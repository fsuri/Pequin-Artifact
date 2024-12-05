#!/usr/bin/env bash


export PATH=$PATH:/usr/lib/postgresql/12/bin
SIZE=20G
# CLUSTERID=pgdata
DATA=~/primary
# /usr/lib/postgresql/12/bin/pg_ctl -D $DATA/db start

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



display_banner "Initializing Postgres Primary Server - Part I" 

# remove previous leftovers...
# /usr/lib/postgresql/12/bin/pg_ctl -D $DATA/db stop
rm -r $DATA
mkdir -p $DATA/db $DATA/log || true
sudo mount -t tmpfs -o size=$SIZE,nr_inodes=10k,mode=0777 tmpfs $DATA

/usr/lib/postgresql/12/bin/initdb -D $DATA/db
	
# sudo rsync -avz $DATA/db/ $DATA/dbinit/     ---- what does thi do?
	
CONF=$DATA/db/postgresql.conf

# 	1. change listen addresses to '*'
cat $CONF | grep -n  listen
m_line=$(cat $CONF | grep -n  listen | cut -d: -f1)
sudo sed -i "${m_line}s/localhost/*/g" $CONF
sudo sed -i "${m_line}s/#/""/" $CONF
cat $CONF| grep -n  listen
		
# 	2. uncomment port and choose one, e.g.: 5433
cat $CONF | grep -n "port ="
m_line=$(cat $CONF | grep -n  "port =" | cut -d: -f1)
sudo sed -i "${m_line}s/5432/5433/g" $CONF
sudo sed -i "${m_line}s/#/""/" $CONF
cat $CONF| grep -n  "port ="
		
# 	3. disable logging
cat $CONF | grep -n "log_min_messages"
m_line=$(cat $CONF | grep -n  "log_min_messages" | cut -d: -f1)
sudo sed -i "${m_line}s/warning/panic/g" $CONF
sudo sed -i "${m_line}s/#/""/" $CONF
cat $CONF| grep -n  "log_min_messages"
		
		
sudo chmod a+w /var/run/postgresql
	

