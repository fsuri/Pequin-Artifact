#!/usr/env/bin bash

sudo echo "ssl-cert:x:115" >> /etc/group
sudo apt install postgresql
sudo sed -i '$ d' /etc/group
