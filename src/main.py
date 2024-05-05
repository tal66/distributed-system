import multiprocessing
import pickle
import threading
from concurrent.futures import ThreadPoolExecutor
import socket
import logging
from time import sleep, time

import common
from common import Token
from clients import ClientLibrary, Client
from servers import Server

log = logging.getLogger(__name__)


def init_servers(server_instances: dict):
    # log
    log_msg = f"settings: n={common.System.curr_num_servers}, f={common.System.curr_f}"
    log.info(log_msg)
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    data = pickle.dumps({"msg": log_msg, "file": "main", "sent_from": "init_servers"})
    client_socket.sendto(data, common.MAIN_EVENTS_ADDR)

    # pool
    pool = ThreadPoolExecutor(max_workers=10)

    # start servers
    port = common.SERVER_START_PORT
    for sid in server_instances.keys():
        s = server_instances[sid]
        log.info(f"server {s.id} running on port {port}")
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_address = ('localhost', port)
        server_socket.bind(server_address)
        pool.submit(s.udp_receive, server_socket)

        port += 1

    port = common.SERVER_START_PORT
    for sid in server_instances.keys():
        Server.servers_addr[sid] = ('localhost', port)
        port += 1

    # init server token structures
    for sid in server_instances.keys():
        s: Server = server_instances[sid]
        s.tokenID_to_token = {i: Token(id=i, version=1, owner=f'c{((i - 1) // 10) + 1}') for i in range(1, 61)}
        s.client_to_tokenID_history = {f'c{i}': [k for k in range((i - 1) * 10 + 1, 10 * i + 1)] for i in range(1, 7)}

    pool.shutdown(wait=True)


def init_system(server_instances: dict):
    """start sys server"""
    for i, sid in enumerate(server_instances.keys()):
        common.System.servers_info[sid] = {}
        common.System.servers_info[sid]["addr"] = ('localhost', common.SERVER_START_PORT + i)
        common.System.servers_info[sid]["is_faulty"] = False

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # add faults to servers
    for i in range(1, common.System.curr_f + 1):
        sid = f"s{i}"
        log_msg = f"settings: server {sid} is faulty with probability {common.FAIL_MSG_PROB}"
        common.System.servers_info[sid]["is_faulty"] = True

        # log
        log.info(log_msg)
        data = {"msg": log_msg, "file": "main", "sent_from": "init_servers"}
        data = pickle.dumps(data)
        client_socket.sendto(data, common.MAIN_EVENTS_ADDR)

    # sys server
    sys_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sys_server.bind(common.SYSTEM_ADDR)
    common.System.udp_receive(sys_server)


def init_main_events_logger(filename="main_events.log"):
    log.info(f"main events logger running on port {common.MAIN_EVENTS_PORT}")
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_address = ('localhost', common.MAIN_EVENTS_PORT)
    server_socket.bind(server_address)

    f = open(filename, "w")
    time0 = time()
    while True:
        data, addr = server_socket.recvfrom(1024 * 4)
        data = pickle.loads(data)
        msg = data['msg']
        file = data['file']
        # sent_from = data['sent_from']

        try:
            t = time() - time0
            f.write(f"{t:5.2f} - {file} - {msg}\n")
            f.flush()

        except Exception as e:
            log.error(f"Error in main_events_logger udp_receive: {e} ", exc_info=True)


def init_clients(client_instances: dict, server_instances: dict):
    global token_ids_for_test_run
    log.info(f"init clients={common.System.curr_num_clients}")

    # init clients
    for i, cid in enumerate(client_instances.keys()):
        c = client_instances[cid]
        c.tokens = [common.Token(id=(i * 10) + j, version=1, owner=c.id) for j in range(1, 11)]

    server_ids = [f"s{i}" for i in range(1, common.System.curr_num_servers + 1)]
    ClientLibrary.servers = {}
    port = common.SERVER_START_PORT
    for sid in server_ids:
        ClientLibrary.servers[sid] = ('localhost', port)
        port += 1

    # client lib
    ClientLibrary.clients = client_instances
    ClientLibrary.msgs = set()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_lib_server_address = ('localhost', common.CLIENT_START_PORT)
    server_socket.bind(client_lib_server_address)
    common.System.pool.submit(ClientLibrary.udp_receive, server_socket)

    # periodic
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    common.System.pool.submit(periodic_queue, client_socket)
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    common.System.pool.submit(periodic_report, client_socket, token_ids_for_test_run)

    # test
    common.System.pool.submit(test, client_instances, server_instances)

    # pool.shutdown(wait=True)


def periodic_queue(client_socket: socket.socket, period_sec=5):
    while True:
        sleep(period_sec)
        for port in range(common.SERVER_START_PORT, common.SERVER_START_PORT + common.MAX_NUM_SERVERS + 1):
            data = {"func": "check_queue",
                    "args": [],
                    "send_to": ('localhost', port), "send_to_id": "", "sent_from": "periodic_queue",
                    "delay": 0, "disable_fault": True,
                    }
            data = pickle.dumps(data)
            client_socket.sendto(data, common.SYSTEM_ADDR)


def periodic_report(client_sock, args=(1, 2,), report_delay=20):
    while True:
        sleep(report_delay)
        d = 0
        for port in range(common.SERVER_START_PORT, common.SERVER_START_PORT + common.MAX_NUM_SERVERS + 1):
            data = {"func": "report",
                    "args": (args,),
                    "send_to": ('localhost', port), "send_to_id": "", "sent_from": "periodic_report",
                    "delay": d, "disable_fault": True,
                    }
            data = pickle.dumps(data)
            client_sock.sendto(data, common.SYSTEM_ADDR)
            d += 0.1


def start_new_server(server_instances):
    sid = "s0"
    addr = ('localhost', 0)
    for i in range(1, common.MAX_NUM_SERVERS + 1):
        sid = f"s{i}"
        if sid not in server_instances:
            addr = ('localhost', common.SERVER_START_PORT + i - 1)
            break

    log.info(f"new server {sid} running on {addr}")
    s = Server(id=sid, addr=addr)
    server_instances[sid] = s
    s.servers_addr = {sid: server_instances[sid].addr for sid in server_instances.keys()}

    s.is_ready = False
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind(addr)
    threading.Thread(target=s.udp_receive, args=(server_socket,)).start()
    return sid, addr


def test(client_instances: dict[str, Client], server_instances: dict[str, Server]):
    log.info("starting test")

    # !! set token_ids for periodic report BEFORE running test
    #    run each scenario SEPARATELY (comment out other tests)

    ## test 1: pay + get_tokens
    test_1(client_instances)

    ## test 2: transform c6 to server + pay
    # test2(client_instances, server_instances)

    ## test 3: transform s1 to client + pay to new client + read
    # test3(client_instances)


def test_1(client_instances):
    ## test 1: pay, get_tokens
    client_instances['c1'].pay(1, 1, "c2")
    sleep(7)
    client_instances['c1'].get_tokens(owner_id="c1")
    sleep(7)
    client_instances['c2'].pay(14, 1, "c3")


def test3(client_instances: dict[str, Client]):
    ## test 3: transform s1 to client + pay to new client + read
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    new_client = 'c7'
    client_instances[new_client] = Client(new_client, [])
    data = pickle.dumps({"func": "rm_server_and_add_client",
                         "kwargs": {"server_to_rm": 's1', 'new_client': new_client},
                         "send_to": common.SYSTEM_ADDR, "send_to_id": "", "sent_from": "test", })
    client_socket.sendto(data, common.SYSTEM_ADDR)

    sleep(4)
    client_instances['c2'].pay(14, 1, "c7")
    sleep(4)
    client_instances['c7'].get_tokens(owner_id="c7")

def test2(client_instances: dict[str, Client], server_instances):
    # test 2: transform c6 to server + pay
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sid, addr = start_new_server(server_instances)
    log.info(f"new server {sid} addr {addr} started")
    client_to_rm = 'c6'
    # client_instances['c6'].pop()
    data = pickle.dumps({"func": "add_server_and_rm_client",
                         "kwargs": {"client_to_rm": client_to_rm, 'new_server': {'sid': sid, 'addr': addr}},
                         "send_to": common.SYSTEM_ADDR, "send_to_id": "", "sent_from": "test", })
    client_socket.sendto(data, common.SYSTEM_ADDR)

    client_instances['c1'].pay(1, 1, "c2")


# token_ids for periodic report in test run
token_ids_for_test_run = [1, 14]

if __name__ == '__main__':
    server_instances = {f"s{i}": Server(id=f"s{i}", addr=('localhost', common.SERVER_START_PORT + i - 1)) for i in
                        range(1, common.System.curr_num_servers + 1)}
    client_instances = {f"c{i}": Client(id=f"c{i}") for i in range(1, common.System.curr_num_clients + 1)}
    main_events_log_filename = "main_events.log"
    # log.info(f"num cpu : {multiprocessing.cpu_count()}")

    procs = [multiprocessing.Process(target=init_main_events_logger, args=(main_events_log_filename,)),
             multiprocessing.Process(target=init_servers, args=(server_instances,)),
             multiprocessing.Process(target=init_system, args=(server_instances,)),
             multiprocessing.Process(target=init_clients, args=(client_instances, server_instances))
             ]

    for p in procs:
        p.start()
        sleep(0.3)

    for proc in procs:
        proc.join()
