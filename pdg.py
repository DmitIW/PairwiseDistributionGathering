import sys
import signal
import time

from logging import info

from config import (
    get_clickhouse_url, get_clickhouse_db,
    get_tarantool_url, get_tarantool_port,
    get_time_offset, get_expiration_time, get_updating_interval
)
from flow import (
    ch_connection, exec_query,
    Src2Dst, Dst2Src, Dst2Proto,
    ClickhouseAgent
)
from flow import (
    Src2DstAttack, Src2DstLegal,
    Dst2SrcAttack, Dst2SrcLegal,
    Dst2ProtoAttack, Dst2ProtoLegal
)
from flow import (
    create_flow
)

from processing import (
    get_and_store
)

from utility import (
    current_time
)

pairs = [
    (Src2Dst, (Src2DstLegal, Src2DstAttack)),
    (Dst2Src, (Dst2SrcLegal, Dst2SrcAttack)),
    (Dst2Proto, (Dst2ProtoLegal, Dst2ProtoAttack))
]
ATTACK_POS = 1
LEGAL_POS = 0


def main():
    clickhouse_connection_args = {
        "database_name": get_clickhouse_db(),
        "url": get_clickhouse_url(),
        "username": "default"
    }

    tarantool_space_args = {
        "url": get_tarantool_url(),
        "port": get_tarantool_port(),
        "expiration_time": get_expiration_time()
    }

    time_offset = get_time_offset()

    global ATTACK_POS, LEGAL_POS, pairs
    flows = []
    for pair in pairs:
        source = pair[0]
        for position, destination in enumerate(pair[1]):
            attack = position == ATTACK_POS
            clickhouse_connection_args["query_model"] = source
            ch_agent = ClickhouseAgent(**clickhouse_connection_args)
            flows.append(
                create_flow(ch_agent, destination(**tarantool_space_args), time_offset=time_offset, attack=attack))

    updating_interval_in_seconds = get_updating_interval()
    while True:
        start_time = current_time()
        print(f"MainLoop:info: start processing")

        for flow in flows:
            with flow as context:
                source = context[0]
                destination = context[1]
                get_and_store(source.exec_query(**flow.get_arguments()),
                              destination, lambda x: x.row()[:-1])

        elapsed_time = current_time() - start_time
        sleep_time = max(0, updating_interval_in_seconds - elapsed_time)
        print(f"MainLoop:info: end processing; elapsed time: {elapsed_time} seconds")
        print(f"MainLoop:info: next processing iteration after: {sleep_time} seconds")
        time.sleep(sleep_time)


def exit_gracefully(signum, frame):
    sys.exit(0)


if __name__ == "__main__":
    print("Gathering start")
    signal.signal(signal.SIGINT, exit_gracefully)
    main()
    print("Gathering end")
