#!/usr/bin/env bash


export PATH=$PATH:/usr/lib/postgresql/12/bin
SIZE=20G
DATA=~/replica
PGV=12
USER=postgres
# PRIMARY=1.1.1.1 

display_banner() {
    local banner_text="$1"
    echo "*****************************************"
    echo "*                                       *"
    echo "          $banner_text"
    echo "*                                       *"
    echo "*****************************************"
}

display_banner "Initializing Postgres Replica Server" 

id=$1
echo "id is $id"
primary_ip=$(getent hosts "us-east-1-0" | awk '{ print $1 }')
echo "primarty:"
echo $primary_ip


# remove previous leftovers...
rm -r $DATA
mkdir -p $DATA/db $DATA/log || true
chmod 700 $DATA/db
	
/usr/lib/postgresql/12/bin/pg_basebackup -h $primary_ip -U repuser --checkpoint=fast -D $DATA/db -R --slot=rep_slot_2_$id -C --port=5433

sudo chmod a+w /var/run/postgresql

/usr/lib/postgresql/12/bin/pg_ctl -D $DATA/db start



# psql postgres -h localhost -p 5433, if wish to connect
	


	# /usr/lib/postgresql/12/bin/pg_ctl -D ~/replica/db stop

	

	