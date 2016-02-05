#!/bin/bash
cd $(dirname $0)
python parallel_consume.py cep_storm_1 &
python parallel_consume.py cep_storm_2 &
python parallel_consume.py cep_storm_3 &

