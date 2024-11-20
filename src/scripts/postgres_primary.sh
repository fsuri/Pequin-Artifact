#!/usr/bin/env bash


export PATH=$PATH:/usr/lib/postgresql/12/bin
SIZE=20G
# CLUSTERID=pgdata
DATA=~/primary
# /usr/lib/postgresql/12/bin/pg_ctl -D $DATA/db start

# echo $DATA
PGV=12
USER=postgres



replica_servers=("us-east-1-1" "us-east-1-2" "eu-west-1-0")
# replica_servers=("us-east-1-1")



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
/usr/lib/postgresql/12/bin/pg_ctl -D $DATA/db stop
rm -r $DATA
mkdir -p $DATA/db $DATA/log || true
# sudo mount -t tmpfs -o size=$SIZE,nr_inodes=10k,mode=0777 tmpfs $DATA

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
		


# # 	3. change to sync replication
# cat $CONF | grep -n  synchronous_commit
# m_line=$(cat $CONF | grep -n  synchronous_commit | cut -d: -f1)
# sudo sed -i "${m_line}s/#/""/" $CONF
# cat $CONF| grep -n  synchronous_commit
		
# cat $CONF | grep -n "synchronous_standby_names"
# m_line=$(cat $CONF | grep -n  "synchronous_standby_names" | cut -d: -f1)
# sudo sed -i "${m_line}s/''/'*'/g" $CONF
# sudo sed -i "${m_line}s/#/""/" $CONF
# cat $CONF| grep -n  "synchronous_standby_names"
		
sudo chmod a+w /var/run/postgresql
	


# This part doesnot work from script, works from terminal
# /usr/lib/postgresql/12/bin/pg_ctl -D $DATA/db stop
# sleep 10
# /usr/lib/postgresql/12/bin/pg_ctl -D $DATA/db start


# # psql postgres -h localhost -p 5433 
# # 	create user repuser WITH PASSWORD '123' replication;
# echo "CREATE USER repuser WITH PASSWORD '123' REPLICATION" | psql postgres -h localhost -p 5433 

# # nano ~/tmp2/primary_db/pg_hba.conf :
# # 	add line per each follower with its ip
# # 	example:
# # 		host    replication   repuser  128.105.144.165/24   trust
# # 						<replica ip>
# for server_name in "${replica_servers[@]}"; do
#     ip_address=$(getent hosts "$server_name" | awk '{ print $1 }')
#     echo $ip_address
#     sudo sed -i "\$a\host    replication             repuser             ${ip_address}/24                       trust" /users/shir/primary/db/pg_hba.conf

# done


# sleep 10
	
# /usr/lib/postgresql/12/bin/pg_ctl -D $DATA/db restart



# # Setting the system
# setting_system
# setting_db db1?



# How to remove all repl slots:

# DO $$ 
# DECLARE 
#     slot RECORD; 
# BEGIN 
#     FOR slot IN SELECT slot_name FROM pg_replication_slots LOOP 
#         EXECUTE format('SELECT pg_drop_replication_slot(%L)', slot.slot_name); 
#     END LOOP; 
# END $$;


# SELECT * FROM pg_replication_slots; 