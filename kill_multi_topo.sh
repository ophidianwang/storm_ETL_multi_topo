#!/bin/bash
cd $(dirname $0)
source /home/vagrant/petrel_env/bin/activate
./topo_1/cluster_kill.sh &
./topo_2/cluster_kill.sh &
./topo_3/cluster_kill.sh &
