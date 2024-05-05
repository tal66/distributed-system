import logging
import multiprocessing
import pickle
import socket
from copy import copy

import common
from common import ClientMessage, ServerMessage, System, Token

log = logging.getLogger(__name__)


class Server:
    system_addr = common.SYSTEM_ADDR
    client_lib_addr = ('localhost', common.CLIENT_START_PORT)
    servers_addr = {}

    def __init__(self, id, tokenID_to_token: dict = None, addr=None):
        self.id = id
        self.tokenID_to_token: dict[int, Token] = tokenID_to_token or {}
        self.client_to_tokenID_history = {}
        self.msgs = {}
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # to send messages
        self.curr_num_servers = System.curr_num_servers
        self.queue_read = multiprocessing.Queue()
        self.is_ready = True
        self.addr = addr
        self.shutdown_flag = False

    def pay(self, msg_id: str, client_id, token_id, version, new_owner_id):
        log.debug(f"server {self.id} received req: {client_id} pay token{token_id} to {new_owner_id}")
        if msg_id in self.msgs:
            log.debug(f"server {self.id} already processed {msg_id}")
            return

        token = self.tokenID_to_token[token_id]

        # validate
        if token.owner != client_id:
            log.error(f"Error: {self.id} token mismatch owner [{token.owner}] != client [{client_id}]")
            self.write_to_main_events_log(
                f"Error: {self.id} token mismatch owner [{token.owner}] != client [{client_id}]")
            return

        self.msgs[msg_id] = {}
        self.msgs[msg_id]['num_servers'] = self.curr_num_servers

        if not token.locked:
            log.debug(f"server {self.id} lock token {token.id} (reason: pay req)")
            token.locked = True

        self.broadcast_echo(
            ServerMessage(self.id, "pay", ClientMessage(client_id, "pay", msg_id, token_id, version, new_owner_id)))

    def get_tokens(self, msg_id, client_id, owner_id):
        log.debug(f"server {self.id} received: get tokens for owner {owner_id}")

        if self.is_client_msg_done(msg_id):
            return

        # save / reset
        self.msgs[msg_id] = {'count': 0, 'done': False, 'tokens': {}, 'num_servers': self.curr_num_servers}

        # request client tokens from all servers
        client_msg = ClientMessage(client_id=client_id, client_msg_type="get_tokens",
                                   msg_id=msg_id, owner_id=owner_id)
        self.broadcast_read_tokens(
            ServerMessage(server_id=self.id, server_msg_type="get_tokens", client_msg=client_msg))

    ###
    ###  from here on - msgs to/from other servers / internal functions
    ###

    def receive_read_tokens(self, msg: ServerMessage):
        log.debug(f"server {self.id} receive_read_tokens: {msg}")
        msg_id = msg.client_msg.msg_id

        owner_id = msg.client_msg.owner_id

        if msg.server_id != self.id:
            # send client's tokens to requesting server
            s = self.servers_addr[msg.server_id]
            tokens = self.get_owner_tokens_history_at_curr_server(owner_id)

            # edit msg (add tokens, not changing server_id)
            msg.tokens = tokens
            data = {"func": "receive_read_tokens",
                    "args": msg,
                    "send_to": s, "send_to_id": msg.server_id, "sent_from": self.id
                    }
            self.client_socket.sendto(pickle.dumps(data), self.system_addr)
        else:  # received answer for my query
            msg_info = self.msgs[msg_id]
            msg_info['count'] += 1

            # update tokens status in info
            for token in msg.tokens:
                if token.id not in msg_info['tokens']:
                    msg_info['tokens'][token.id] = copy(token)
                else:
                    curr_v = msg_info['tokens'][token.id].version
                    if token.version > curr_v:
                        # update if newer
                        msg_info['tokens'][token.id] = copy(token)
                    elif token.version == curr_v:
                        if token.locked:
                            msg_info['tokens'][token.id].locked = True
                        if token.owner != msg_info['tokens'][token.id].owner:
                            log.error(
                                f"Error: {self.id} token mismatch owner {token} != {msg_info['tokens'][token.id]}")

            if msg_info['count'] == self.majority_count(msg_info['num_servers']):
                log.info(f"{self.id} received majority of tokens for owner {owner_id}")
                tokens_answer = []
                for token in msg_info['tokens'].values():
                    if token.owner != owner_id:
                        continue
                    if token.locked:
                        self.queue_read.put(
                            {"msg_id": msg_id, "client_id": msg.client_msg.client_id, "owner_id": owner_id}
                        )
                        log.info(
                            f"{self.id} postpone and reset get_tokens({owner_id}) (token {token} locked). queue size={self.queue_read.qsize()}")
                        return  # postponed
                    tokens_answer.append(token)

                # send to client
                self.client_socket.sendto(pickle.dumps({
                    "func": "receive_message",
                    "args": ServerMessage(self.id, "get_tokens", msg.client_msg, tokens=tokens_answer),
                    "send_to": self.client_lib_addr, "send_to_id": "client_lib",
                    "sent_from": self.id
                }), self.system_addr)

                self.mark_done(msg.client_msg)
                log.info(f"{self.id} get_tokens({owner_id}) done: {[t.id for t in tokens_answer]}")
                self.write_to_main_events_log(f"{self.id} get_tokens({owner_id}) done: {[t.id for t in tokens_answer]}")

    def broadcast_read_tokens(self, server_msg: ServerMessage):
        """request tokens of owner"""
        log.debug(f"server {self.id} broadcast_read_tokens")
        # send done to all
        for sid in self.servers_addr.keys():
            if sid == self.id:
                srv_msg = copy(server_msg)
                srv_msg.tokens = self.get_owner_tokens_history_at_curr_server(server_msg.client_msg.owner_id)
                self.receive_read_tokens(srv_msg)
            else:
                s = self.servers_addr[sid]
                data = {"func": "receive_read_tokens",
                        "args": server_msg,
                        "send_to": s, "send_to_id": sid, "sent_from": self.id
                        }
                self.client_socket.sendto(pickle.dumps(data), self.system_addr)

    def get_owner_tokens_history_at_curr_server(self, owner_id: str):
        history = self.client_to_tokenID_history[owner_id]
        tokens = [self.tokenID_to_token[token_id] for token_id in history]
        return tokens

    def get_owner_tokens_at_curr_server(self, owner_id: str):
        tokens = [token for token in self.tokenID_to_token.values() if token.owner == owner_id]
        return tokens

    def majority_count(self, n=None):
        """f < n/2 therefore n-f > n/2"""
        if (n is None) or (n > self.curr_num_servers):
            return self.curr_num_servers // 2 + 1

        return n // 2 + 1

    def execute_pay(self, msg: ServerMessage):
        token = self.tokenID_to_token[msg.client_msg.token_id]

        # log
        log.info(f"{self.id} executing PAY {msg.client_msg.client_id}({token.id}) -> {msg.client_msg.new_owner}: {msg}")
        self.write_to_main_events_log(
            f"{self.id} executing PAY {msg.client_msg.client_id} -> {msg.client_msg.new_owner}: {msg}")

        # update token
        token.version += 1
        token.owner = msg.client_msg.new_owner
        log.debug(f"{self.id} unlock token {token.id}")
        token.locked = False
        self.client_to_tokenID_history[token.owner].append(token.id)

        # mark as done
        self.mark_done(msg.client_msg)

        # send done to all
        self.broadcast_done(msg)

        # response to client
        self.client_socket.sendto(pickle.dumps({
            "func": "receive_message",
            "args": ServerMessage(self.id, "ok", msg.client_msg, token),
            "send_to": self.client_lib_addr, "send_to_id": "client_lib",
            "sent_from": self.id
        }), self.system_addr)

    def broadcast_done(self, msg):
        token = self.tokenID_to_token[msg.client_msg.token_id]
        for sid in self.servers_addr.keys():
            if sid == self.id:
                self.receive_done_pay(ServerMessage(self.id, "done", msg.client_msg, token))
            else:
                s = self.servers_addr[sid]
                data = {"func": "receive_done_pay",
                        "args": ServerMessage(self.id, "done", msg.client_msg, token),
                        "send_to": s, "sent_from": self.id, "send_to_id": sid
                        }
                self.client_socket.sendto(pickle.dumps(data), self.system_addr)

    def receive_done_pay(self, msg: ServerMessage):
        log.debug(f"{self.id} received done: {msg}")

        if msg.server_id != self.id:
            # execute if not done already
            if not self.is_client_msg_done(msg.client_msg):
                log.info(f"{self.id} heard done pay from {msg.server_id} before getting majority echo")
                self.write_to_main_events_log(f"{self.id} heard 'done pay' from {msg.server_id}")
                self.execute_pay(msg)
            else:
                log.debug(f"{self.id} pay already done")

    def receive_echo(self, msg: ServerMessage):
        log.debug(f"server {self.id} received echo: {msg}")

        if msg.server_id != self.id:  # received echo msg of another server
            # echo back (msg as is) to sender
            log.debug(f"server {self.id} return echo to {msg.server_id}")
            s = self.servers_addr[msg.server_id]
            data = {"func": "receive_echo", "args": msg, "send_to": s, "sent_from": self.id,
                    "send_to_id": msg.server_id}
            self.client_socket.sendto(pickle.dumps(data), self.system_addr)

            # lock token if "pay" msg
            if msg.client_msg.client_msg_type == "pay":
                token = self.tokenID_to_token[msg.client_msg.token_id]
                if not self.is_client_msg_done(msg.client_msg) and not token.locked:
                    log.debug(f"server {self.id} lock token {token.id} (reason: pay echo)")
                    token.locked = True

        else:  # received echo regarding my own message
            # if done - return
            if self.is_client_msg_done(msg.client_msg):
                return

            # update echo count
            msg_id = msg.client_msg.msg_id

            msg_info = self.msgs[msg_id]
            if not msg_info.get('count'):
                msg_info['count'] = 0
            msg_info['count'] += 1
            log.debug(f"server {self.id} {msg.client_msg.client_msg_type} echo count : {msg_info['count']}")

            # if heard majority
            if msg_info['count'] == self.majority_count(msg_info['num_servers']):
                log.info(f"{self.id} heard back from majority")

                self.write_to_main_events_log(f"{self.id} heard back from majority")

                if not self.is_client_msg_done(msg.client_msg):
                    self.execute_pay(msg)

    def broadcast_echo(self, msg: ServerMessage):
        log.debug(f"server {self.id} echo: {msg}")

        # echo to all
        for sid in self.servers_addr.keys():
            log.debug(f"server {self.id} sending echo to {sid}")
            if sid == self.id:
                self.receive_echo(msg)
            else:
                s = self.servers_addr[sid]
                data = {"func": "receive_echo", "args": msg,
                        "send_to": s, "send_to_id": sid, "sent_from": self.id
                        }
                self.client_socket.sendto(pickle.dumps(data), self.system_addr)

    def is_client_msg_done(self, msg: str or ClientMessage):
        if type(msg) is ClientMessage:
            msg_id = msg.msg_id
        else:
            msg_id = msg
        return self.msgs.get(msg_id) and self.msgs[msg_id].get('done')

    def mark_done(self, msg: ClientMessage):
        # log.info(f"server {self.id} marking done: {msg}")
        msg_id = msg.msg_id
        if not self.msgs.get(msg_id):
            self.msgs[msg_id] = {}
        self.msgs[msg_id]['done'] = True

    def write_to_main_events_log(self, msg: str):
        data = {"msg": msg,
                "file": "servers",
                "sent_from": self.id
                }
        data = pickle.dumps(data)
        self.client_socket.sendto(data, common.MAIN_EVENTS_ADDR)

    def report(self, token_ids: list):
        if not self.is_ready:
            log.info(f"server {self.id} not ready to report")
            return

        report_list = [f"server {self.id} tokens report:"]
        for token_id in token_ids:
            token = self.tokenID_to_token[token_id]
            report_list.append(f"{token}")

        log.info(' '.join(report_list))
        self.write_to_main_events_log(' '.join(report_list))

    def check_queue(self):
        if self.queue_read.qsize() > 0:
            item = self.queue_read.get()
            log.info(f"{self.id} retry from queue: get_tokens {item}")
            self.get_tokens(**item)

    def udp_receive(self, server_socket: socket.socket):
        while True:
            if self.shutdown_flag:
                log.info(f"{self.id} shutdown")
                self.write_to_main_events_log(f"{self.id} shutdown")
                break

            data, addr = server_socket.recvfrom(1024 * 4)
            data = pickle.loads(data)
            args = data['args']
            if data['func'] != "check_queue":
                log.debug(f"server {self.id} received from {data['sent_from']} data {data}")
                log.debug(f"calling {data['send_to_id']}.{data['func']} with args: {args}")
            # log.info(f"args: {args}, type: {type(args)}")

            try:
                func = getattr(self, data['func'])
                if type(args) is dict:
                    func(**args)
                elif type(args) is list or type(args) is tuple:
                    func(*args)
                else:
                    func(args)

            except Exception as e:
                log.error(f"Error in udp_receive: {e} ", exc_info=True)

    def added_server_event(self, new_sid, addr):
        log.debug(f"{self.id} received 'server added event': {new_sid} at {addr}")
        self.curr_num_servers += 1
        self.servers_addr[new_sid] = addr

        # need to send tokens to sync new server.
        # temporary: for now sync immediately with just non-faulty servers
        if self.id in ['s4', 's5', 's6', 's7']:
            data = {"func": "sync", "args": (self.tokenID_to_token, self.client_to_tokenID_history),
                    "send_to": addr, "send_to_id": new_sid, "sent_from": self.id, "delay": 0, "disable_fault": True}
            self.client_socket.sendto(pickle.dumps(data), self.system_addr)

    def removed_server_event(self, server_to_rm, new_client):
        log.debug(f"{self.id} received 'server removed event': {server_to_rm}")
        self.curr_num_servers -= 1
        if self.servers_addr.get(server_to_rm):
            self.servers_addr.pop(server_to_rm)
        self.client_to_tokenID_history[new_client] = []
        if server_to_rm == self.id:
            self.shutdown_flag = True

    def sync(self, tokenID_to_token, client_to_tokenID_history):
        # for now sync with just one server (should be at least f+1)
        if not self.is_ready:
            log.info(f"{self.id} sync")
            self.tokenID_to_token = tokenID_to_token
            self.client_to_tokenID_history = client_to_tokenID_history
            self.is_ready = True
