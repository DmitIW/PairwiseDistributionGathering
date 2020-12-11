import sys
import signal
import time

from config import (
    get_clickhouse_url, get_clickhouse_db,
    get_tarantool_url, get_tarantool_port,
    get_time_offset, get_expiration_time, get_updating_interval
)
from flow import (
    ch_connection, exec_query,
    Src2Dst, Dst2Src, Dst2Proto
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
    clickhouse_database_conn = ch_connection(
        database_name=get_clickhouse_db(),
        url=get_clickhouse_url(),
        username="default"
    )

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
            flows.append(
                create_flow(source, destination(**tarantool_space_args), time_offset=time_offset, attack=attack))

    updating_interval_in_seconds = get_updating_interval()
    while True:
        start_time = current_time()
        print("Another loop iteration start")
        for flow in flows:
            get_and_store(exec_query(clickhouse_database_conn, flow.get_source(), **flow.get_arguments()),
                          flow.get_destination(), lambda x: x.row()[:-1])
        elapsed_time = current_time() - start_time
        print(f"Another loop iteration over; elapsed: {elapsed_time}")
        time.sleep(updating_interval_in_seconds)


def exit_gracefully(signum, frame):
    sys.exit(0)


if __name__ == "__main__":
    print("Gathering start")
    signal.signal(signal.SIGINT, exit_gracefully)
    main()
    print("Gathering end")
