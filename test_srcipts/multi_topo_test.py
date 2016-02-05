# -*- coding: utf-8 -*-

import os
import re
import time
from datetime import datetime
import subprocess
from pymongo import MongoClient


def get_log_times(spout_name):
    """
    :param spout_name:
    :return:
    parse storm log and return several timestamp
    """
    log_path = "/realtime/storm_log/storm_petrel_{0}_{1}.log"
    today = datetime.now().strftime("%Y%m%d")
    spout_log_path = log_path.format(spout_name, today)

    consume_start_timestamp = 0
    consume_end_timestamp = 0
    emit_end_timestamp = 0
    consume_count = 0
    emit_count = 0
    with open(spout_log_path, "r") as f:
        for line in f:
            line = line.strip()
            if line.find("start") != -1:
                consume_start_timestamp = float(line.split(" ")[-2])
            elif line.find("finish") != -1:
                consume_end_timestamp = float(line.split(" ")[-2])
                consume_count = int(line.split(" ")[3])
            elif line.find("emit") != -1:
                emit_end_timestamp = float(line.split(" ")[-2])
                emit_count = int(line.split(" ")[3])
    print("---")
    print("{0} start consuming records at {1}".format(
        spout_name, datetime.fromtimestamp(consume_start_timestamp).strftime("%Y-%m-%d %H:%M:%S")))
    print("{0} finish consuming {1} records at {2}".format(
        spout_name, consume_count, datetime.fromtimestamp(consume_end_timestamp).strftime("%Y-%m-%d %H:%M:%S")))
    print("{0} finish emit {1} tuples at {2}".format(
        spout_name, emit_count, datetime.fromtimestamp(emit_end_timestamp).strftime("%Y-%m-%d %H:%M:%S")))
    print("{0} spend {1} seconds consuming {2} messages".format(
        spout_name, consume_end_timestamp - consume_start_timestamp, consume_count))
    print("{0} spend {1} seconds emitting {2} tuples".format(
        spout_name, emit_end_timestamp - consume_start_timestamp, emit_count))
    print("---")
    return [consume_start_timestamp, consume_end_timestamp, emit_end_timestamp, consume_count, emit_count]


def main():
    client = MongoClient('172.28.128.22', 40000)
    db = client.cep_storm
    collection = db.lte_pgw_exp

    topology_path = os.path.dirname(os.path.abspath(__file__)) + "/../"
    create_py_path = "create.py"
    submit_sh = "cluster_run.sh"
    kill_sh = "cluster_kill.sh"
    kill_all_sh = topology_path + "topo_1/kill_GenericTopology.sh"

    # kill topology
    subprocess.call(["bash", topology_path + "topo_1/" + kill_sh])
    subprocess.call(["bash", topology_path + "topo_2/" + kill_sh])
    subprocess.call(["bash", topology_path + "topo_3/" + kill_sh])
    time.sleep(25)
    subprocess.call(["bash", kill_all_sh])
    time.sleep(5)
    # to make sure topology is over
    print("kill topology done at {0}.".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    # clear mongodb records
    collection.delete_many({})
    print("clear collection done at {0}.".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    check_count = collection.count()
    if check_count != 0:
        print("error happened, collection.count() is not 0")
        raise SystemExit("mongodb error")

    # start topology
    subprocess.call(["bash", topology_path + "topo_1/" + submit_sh])
    subprocess.call(["bash", topology_path + "topo_2/" + submit_sh])
    subprocess.call(["bash", topology_path + "topo_3/" + submit_sh])
    print("submit topology done at {0}.".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    time.sleep(5)  # topology deploying
    print("start monitoring mongodb at {0}.".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    # then check mongodb, out input data should be remodeled from 1085420 to 570720
    row_count = 0
    row_progress = 0
    dump_start_timestamp = 0
    over_counter = 0
    diff_threshold = 1000
    tolerance_count = 300
    sleep_time = 0.1
    tolerance_time = tolerance_count * sleep_time

    while True:
        row_count = collection.count()  # get current row count
        if dump_start_timestamp == 0 and row_count != 0:
            dump_start_timestamp = time.time()  # mark timestamp which means start writing
        diff = row_count - row_progress
        if diff > diff_threshold:
            print("There are {0} records in mongodb now.".format(row_count))
            row_progress = row_count
            over_counter = 0
        elif row_count != 0:
            over_counter += 1
            if over_counter > 150:
                print("over_counter: {0}".format(over_counter))

        if over_counter >= tolerance_count:
            break
        time.sleep(sleep_time)  # in case too many queries affect mongodb performance...
    """
    time.sleep(180)
    """
    dump_end_timestamp = time.time() - tolerance_time

    parse_topo_1 = get_log_times("ExpSpout1")
    parse_topo_2 = get_log_times("ExpSpout2")
    parse_topo_3 = get_log_times("ExpSpout3")

    consume_start_timestamp = min(parse_topo_1[0], parse_topo_2[0], parse_topo_3[0])
    consume_end_timestamp = max(parse_topo_1[1], parse_topo_2[1], parse_topo_3[1])
    emit_end_timestamp = max(parse_topo_1[2], parse_topo_2[2], parse_topo_3[2])
    consume_count = parse_topo_1[3] + parse_topo_2[3] + parse_topo_3[3]
    emit_count = parse_topo_1[4] + parse_topo_2[4] + parse_topo_3[4]

    if consume_end_timestamp > dump_end_timestamp or consume_end_timestamp < consume_start_timestamp:
        raise Exception("topology stuck at somewhere.")

    print("===")
    print("Spout start consuming {0} records at {1}".format(
        consume_count, datetime.fromtimestamp(consume_start_timestamp).strftime("%Y-%m-%d %H:%M:%S")))
    print("Spout finish consuming {0} records at {1}".format(
        consume_count, datetime.fromtimestamp(consume_end_timestamp).strftime("%Y-%m-%d %H:%M:%S")))
    print("Spout finish emit {0} tuples at {1}".format(
        emit_count, datetime.fromtimestamp(emit_end_timestamp).strftime("%Y-%m-%d %H:%M:%S")))
    print("Bolt start dumping records at {0}".format(
        datetime.fromtimestamp(dump_start_timestamp).strftime("%Y-%m-%d %H:%M:%S")))
    print("Bolt finish dumping records at {0}".format(
        datetime.fromtimestamp(dump_end_timestamp).strftime("%Y-%m-%d %H:%M:%S")))
    print("===")
    print("re-model into {0} records".format(row_count))
    print("Spout spend {0} seconds consuming {1} messages".format(
        consume_end_timestamp - consume_start_timestamp, consume_count))
    print("Spout spend {0} seconds emitting {1} tuples".format(
        emit_end_timestamp - consume_start_timestamp, emit_count))
    print("Bolt spend {0} seconds dumping {1} records.".format(dump_end_timestamp - dump_start_timestamp, row_count))
    print("The 1th record walks through the topology in {0} seconds".format(
        dump_start_timestamp - consume_start_timestamp))
    print("The {0}th record walks through the topology in {1} seconds".format(
        consume_count, dump_end_timestamp - consume_end_timestamp))
    print("total time: {0} seconds".format(dump_end_timestamp - consume_start_timestamp))


if __name__ == "__main__":
    main()