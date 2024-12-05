#!/usr/bin/env bash


export PATH=$PATH:/usr/lib/postgresql/12/bin
SIZE=20G
# CLUSTERID=pgdata
DATA=~/primary
# echo $DATA
PGV=12
USER=postgres



# replica_servers=("us-east-1-1" "us-east-1-2" "eu-west-1-0")
replica_servers=("us-east-1-1")



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
    
    echo "CREATE USER pequin_user WITH PASSWORD '123'" | psql postgres -h localhost -p 5433 
    echo "ALTER SYSTEM SET log_statement TO 'none'" | psql postgres -h localhost -p 5433 
    echo "ALTER SYSTEM SET log_min_error_statement = panic" | psql postgres -h localhost -p 5433 
    
    echo "ALTER SYSTEM SET max_connections = 250" | psql postgres -h localhost -p 5433 
    echo "ALTER SYSTEM SET work_mem = '4GB'" | psql postgres -h localhost -p 5433 
    echo "ALTER SYSTEM SET shared_buffers='30GB'" | psql postgres -h localhost -p 5433 
    echo "ALTER SYSTEM SET effective_cache_size = '4GB'" | psql postgres -h localhost -p 5433 
    echo "ALTER SYSTEM SET effective_io_concurrency = 8" | psql postgres -h localhost -p 5433 
    
    echo "ALTER SYSTEM SET huge_pages = 'try'" | psql postgres -h localhost -p 5433 
    echo "ALTER SYSTEM SET max_locks_per_transaction = 1024" | psql postgres -h localhost -p 5433 



}


setting_db() {
    local dbname="$1"
    echo "Setting $dbname:"

    echo "GRANT pg_read_server_files TO pequin_user" | psql postgres -h localhost -p 5433

    echo "SELECT 'CREATE DATABASE $dbname' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$dbname')\gexec" | psql postgres -h localhost -p 5433 
    echo "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pequin_user" | psql pequin_user -h localhost -p 5433 -d $dbname
    echo "alter default privileges in schema public grant all on tables to pequin_user;" | psql pequin_user -h localhost -p 5433 -d $dbname
    echo "alter default privileges in schema public grant all on sequences to pequin_user" | psql pequin_user -h localhost -p 5433 -d $dbname
    echo "ALTER DATABASE $dbname SET DEFAULT_TRANSACTION_ISOLATION TO SERIALIZABLE " | psql postgres -h localhost -p 5433 
    echo "ALTER DATABASE $dbname SET ENABLE_MERGEJOIN TO FALSE" | psql postgres -h localhost -p 5433 
    echo "ALTER DATABASE $dbname SET ENABLE_HASHJOIN TO FALSE" | psql postgres -h localhost -p 5433 
    echo "ALTER DATABASE $dbname SET ENABLE_NESTLOOP TO TRUE" | psql postgres -h localhost -p 5433 
    # echo "ALTER DATABASE $dbname SET lock_timeout = 100 " | psql postgres -h localhost -p 5433 


    # su - $USER -c "echo \"GRANT pg_read_server_files TO pequin_user;\" | psql -d $dbname"

}



display_banner "Initializing Postgres Primary Server - Part II" 


# psql postgres -h localhost -p 5433 
# 	create user repuser WITH PASSWORD '123' replication;
echo "CREATE USER repuser WITH PASSWORD '123' REPLICATION" | psql postgres -h localhost -p 5433 
setting_system
setting_db db1


# nano ~/tmp2/primary_db/pg_hba.conf :
# 	add line per each follower with its ip
# 	example:
# 		host    replication   repuser  128.105.144.165/24   trust
# 						<replica ip>
for server_name in "${replica_servers[@]}"; do
    ip_address=$(getent hosts "$server_name" | awk '{ print $1 }')
    echo $ip_address
    sudo sed -i "\$a\host    replication             repuser             ${ip_address}/24                       trust" /users/shir/primary/db/pg_hba.conf

done

# Allowing clients connections
sudo sed -i "\$a\host    all             all             0.0.0.0/0                        trust" /users/shir/primary/db/pg_hba.conf
d_line=$(sudo cat /users/shir/primary/db/pg_hba.conf | grep -n  "IPv4 local" | cut -d: -f1)
d_line=$(expr $d_line + 1)
sudo sed -i "${d_line}d" /users/shir/primary/db/pg_hba.conf		


CONF=$DATA/db/postgresql.conf

# 	3. change to sync replication
cat $CONF | grep -n  synchronous_commit
m_line=$(cat $CONF | grep -n  synchronous_commit | cut -d: -f1)
sudo sed -i "${m_line}s/#/""/" $CONF
cat $CONF| grep -n  synchronous_commit
		
cat $CONF | grep -n "synchronous_standby_names"
m_line=$(cat $CONF | grep -n  "synchronous_standby_names" | cut -d: -f1)
sudo sed -i "${m_line}s/''/'*'/g" $CONF
sudo sed -i "${m_line}s/#/""/" $CONF
cat $CONF| grep -n  "synchronous_standby_names"


/usr/lib/postgresql/12/bin/pg_ctl -D $DATA/db restart >/dev/null

