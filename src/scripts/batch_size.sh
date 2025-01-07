if [ "$#" -le 0 ]
then

  echo "Usage: batch_size.sh {batch size}"

else


ls config | while read shard
do
    if [ ${shard} != "local_config" ]
    then
      echo modifying ${shard} for batch size $1
      sed -i "1c\block-size = ${1}" ./config/${shard}/hotstuff.gen.conf
    fi
done

ls config_pghs | while read shard
do
    if [ ${shard} != "local_config" ]
    then
      echo modifying ${shard} for batch size $1
      sed -i "1c\block-size = ${1}" ./config_pghs/${shard}/hotstuff.gen.conf
    fi
done

fi
