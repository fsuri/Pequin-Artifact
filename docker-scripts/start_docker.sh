#!/bin/bash

docker run \
    -it \
    --platform linux/amd64 \
    --mount type=bind,src=.,dst=/home/sintr \
    pequin-docker \
    bash

# to connect to existing container (get container_id with docker ps)
# docker exec -it [container_id] bash
