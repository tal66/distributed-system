import logging
import pickle
import random
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)-8s - %(levelname)-5s - %(process)d (%(threadName)-9s) - %(message)s')
log = logging.getLogger(__name__)

####### conf #######

SERVER_START_PORT = 8000  # up to _ + MAX_NUM_SERVERS
CLIENT_START_PORT = 8016
SYSTEM_PORT = 8029
MAIN_EVENTS_PORT = 8030
SYSTEM_ADDR = ("localhost", SYSTEM_PORT)
MAIN_EVENTS_ADDR = ("localhost", MAIN_EVENTS_PORT)

IS_SYSTEM_DELAY = True
MAX_DELAY_SEC = 6

FAIL_MSG_PROB = 0.80

MIN_NUM_SERVERS = 3
MAX_NUM_SERVERS = 7
MAX_NUM_CLIENTS = 6 + (MAX_NUM_SERVERS - MIN_NUM_SERVERS)
INIT_NUM_SERVERS = 5


####### System #######


class System:
    """ general configuration """
    curr_num_clients = 6  # changing this init value will require changing the tokens ownership struct in main.py
    curr_num_servers = INIT_NUM_SERVERS  # = n
    curr_f = (curr_num_servers - 1) // 2

    servers_info = {}  # [sid]["is_faulty"] = True/False, [sid]["addr"] = (ip, port)

    pool = ThreadPoolExecutor(max_workers=25)
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    @staticmethod
    def udp_receive(server_socket: socket.socket):
        """
        forwards messages from clients and servers to appropriate destination
        """
        log.info(f"System listening on {server_socket.getsockname()}")
        log.info(f"System conf: n={System.curr_num_servers}, f={System.curr_f}, clients={System.curr_num_clients}")
        while True:
            raw_data, addr = server_socket.recvfrom(1024 * 4)
            data = pickle.loads(raw_data)

            send_to = data['send_to']
            send_to_id = data['send_to_id']
            sent_from = data.get('sent_from')

            # message to self
            if send_to == SYSTEM_ADDR:
                func = data.get('func')
                kwargs = data.get('kwargs')
                if func == "add_server_and_rm_client":
                    client = kwargs["client_to_rm"]
                    new_server = kwargs["new_server"]
                    System.add_server_and_rm_client(client, new_server)
                elif func == "rm_server_and_add_client":
                    server_to_rm = kwargs["server_to_rm"]
                    new_client = kwargs["new_client"]
                    System.rm_server_and_add_client(server_to_rm, new_client)

                continue

            try:
                # check faulty servers
                if not data.get("disable_fault"):
                    from_server_status = System.servers_info.get(send_to_id)
                    to_server_status = System.servers_info.get(sent_from)
                    if System._should_drop_msg(from_server_status) or System._should_drop_msg(to_server_status):
                        log.debug(f"System skipping message {sent_from} {send_to_id} (Fault)")
                        continue

                # delay
                delay = 0
                if "delay" in data:
                    delay = data["delay"]
                elif IS_SYSTEM_DELAY:
                    delay = random.uniform(0, MAX_DELAY_SEC)

                # forward msg
                log.debug(f"System forwarding message (delay={delay:.2f}) from {data['sent_from']} to {send_to_id}")
                System.pool.submit(System._submit_with_delay, raw_data, send_to, delay)

            except Exception as e:
                log.error(f"Error in ClientLibrary udp_receive: {e} ", exc_info=True)

    @staticmethod
    def _submit_with_delay(raw_data, send_to, delay):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if delay:
            time.sleep(delay)
        client_socket.sendto(raw_data, send_to)

    @staticmethod
    def _should_drop_msg(s):
        if s and s.get("is_faulty"):
            if random.random() < FAIL_MSG_PROB:
                return True
        return False

    def rm_server_and_add_client(server_to_rm=None, new_client=None):
        """
        transform server to client
        """
        if System.curr_num_servers == MIN_NUM_SERVERS:
            log.info(f"min servers reached")
            return System.curr_num_servers

        log.info(f"rm server, add client {new_client}")

        # notify servers
        for sid in System.servers_info.keys():
            data = {"func": "removed_server_event",
                    "args": (server_to_rm, new_client,),
                    "send_to": System.servers_info[sid]["addr"],
                    "send_to_id": sid,
                    "sent_from": "System"
                    }
            data = pickle.dumps(data)
            log.debug(f"System notifying {sid} about rm server")
            System.pool.submit(System._submit_with_delay, data, System.servers_info[sid]["addr"], 0)

        # notify client_lib
        data = pickle.dumps({"func": "removed_server_event",
                             "args": (server_to_rm, new_client,),
                             "send_to": ('localhost', CLIENT_START_PORT),
                             "send_to_id": "client_lib",
                             "sent_from": "System",
                             "delay": 0
                             })
        System.pool.submit(System._submit_with_delay, data, ('localhost', CLIENT_START_PORT), 0)

        # rm
        del System.servers_info[server_to_rm]

        System.curr_num_servers -= 1
        System.update_faults()

        log_msg = f"rm {server_to_rm}, added client {new_client}. updated num_servers: {System.curr_num_servers}, f: {System.curr_f}"
        log.info(log_msg)
        System.write_to_main_events_log(log_msg)
        return System.curr_num_servers

    @staticmethod
    def add_server_and_rm_client(client_to_rm=None, new_server=None):
        """
        transform client to server
        """
        if System.curr_num_servers == MAX_NUM_SERVERS:
            log.info(f"max servers reached")
            return System.curr_num_servers

        log.info(f"add server, rm client {client_to_rm}")
        new_sid, new_addr = new_server['sid'], new_server['addr']

        # notify servers
        for sid in System.servers_info.keys():
            data = {"func": "added_server_event",
                    "args": [new_sid, new_addr],
                    "send_to": System.servers_info[sid]["addr"],
                    "send_to_id": sid,
                    "sent_from": "System"
                    }
            data = pickle.dumps(data)
            log.debug(f"System notifying {sid} about new server {new_sid}")
            System.pool.submit(System._submit_with_delay, data, System.servers_info[sid]["addr"], 0)

        # notify client_lib
        data = pickle.dumps({"func": "added_server_event",
                             "args": (client_to_rm, new_sid, new_addr,),
                             "send_to": ('localhost', CLIENT_START_PORT),
                             "send_to_id": "client_lib",
                             "sent_from": "System",
                             "delay": 0
                             })
        System.pool.submit(System._submit_with_delay, data, ('localhost', CLIENT_START_PORT), 0)

        # add
        System.servers_info[new_sid] = {"is_faulty": False, "addr": new_addr}

        System.curr_num_servers += 1
        System.update_faults(new_sid)

        log_msg = f"added server {new_sid} at {new_addr}. faulty={System.servers_info[new_sid]['is_faulty']}. updated num_servers: {System.curr_num_servers}, f: {System.curr_f}"
        log.info(log_msg)
        System.write_to_main_events_log(log_msg)

        return System.curr_num_servers

    @staticmethod
    def write_to_main_events_log(msg):
        data = {"msg": msg,
                "file": "common",
                "sent_from": "System"
                }
        data = pickle.dumps(data)
        System.client_socket.sendto(data, MAIN_EVENTS_ADDR)

    @staticmethod
    def update_faults(new_sid=None):
        prev_f = System.curr_f
        System.curr_f = (System.curr_num_servers - 1) // 2

        if new_sid and prev_f > System.curr_f:
            System.servers_info[new_sid]["is_faulty"] = True
        elif prev_f > System.curr_f:
            for sid in System.servers_info.keys():
                if System.servers_info[sid]["is_faulty"]:
                    System.servers_info[sid]["is_faulty"] = False
                    break


####### data classes #######

class Token:
    def __init__(self, id, version=1, owner=None, locked=False):
        self.id: int = id  # unique token identifier
        self.version: int = version  # starts with 1 and increment
        self.owner: str = owner  # current owner (1 to 100)
        self.locked: bool = locked

    def __str__(self):
        locked_str = ', locked' if self.locked else ''
        owner_str = f', owner {self.owner}' if self.owner else ''
        return f"[id {self.id}, v_{self.version}{owner_str}{locked_str}]"

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.id == other.id and self.owner == other.owner


@dataclass
class ServerMessage:
    server_id: str
    server_msg_type: str  # echo, done, ok
    client_msg: "ClientMessage"
    token: Token = None
    tokens: list[Token] = None


@dataclass
class ClientMessage:
    client_id: str
    client_msg_type: str  # pay, get_tokens
    msg_id: str
    # pay args
    token_id: int = None
    version: int = None
    new_owner: str = None
    # get_tokens args
    owner_id: str = None
