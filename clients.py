import logging
import pickle
import socket
import uuid
from typing import List

import common
from common import ServerMessage, Token

log = logging.getLogger(__name__)


class ClientLibrary:
    msgs = set()
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    servers = {}
    clients = {}

    @staticmethod
    def pay(client_id, token_id, version, new_owner_id):
        log_msg = f"ClientLib broadcast to servers: {client_id} pay token_{token_id} to {new_owner_id}"
        log.info(log_msg)
        ClientLibrary.write_to_main_events_log(log_msg)
        msg_id = ClientLibrary.create_msg_id()

        # send to all
        for sid in ClientLibrary.servers.keys():
            log.debug(f"ClientLib send to {sid}: pay token_{token_id} to {new_owner_id}")
            s = ClientLibrary.servers[sid]

            data = {"func": "pay",
                    "args": (msg_id, client_id, token_id, version, new_owner_id),
                    "send_to": s,
                    "send_to_id": sid,
                    "sent_from": "ClientLib"
                    }
            data = pickle.dumps(data)
            ClientLibrary.client_socket.sendto(data, common.SYSTEM_ADDR)

    @staticmethod
    def get_tokens(client_id, owner_id):
        log_msg = f"ClientLib broadcast to servers: {client_id} request get_tokens({owner_id})"
        log.info(log_msg)
        ClientLibrary.write_to_main_events_log(log_msg)
        msg_id = ClientLibrary.create_msg_id()

        for sid in ClientLibrary.servers.keys():
            log.debug(f"ClientLib send to {sid}: {client_id} get_tokens({owner_id})")
            s = ClientLibrary.servers[sid]

            data = {"func": "get_tokens",
                    "args": (msg_id, client_id, owner_id),
                    "send_to": s,
                    "send_to_id": sid,
                    "sent_from": "ClientLib"
                    }
            data = pickle.dumps(data)
            ClientLibrary.client_socket.sendto(data, common.SYSTEM_ADDR)

    @staticmethod
    def receive_message(msg: ServerMessage):
        log.debug(f"ClientLib received message: {msg}")

        # check if already processed
        if msg.client_msg.msg_id in ClientLibrary.msgs:
            log.debug(f"ClientLib already processed {msg.client_msg.msg_id}")
            return

        # forward to client
        client = ClientLibrary.clients[msg.client_msg.client_id]
        ClientLibrary.write_to_main_events_log(f"ClientLib {msg.server_msg_type} to {client.id}: {msg}")
        client.receive_message(msg)

        # mark as done
        ClientLibrary.msgs.add(msg.client_msg.msg_id)

    @staticmethod
    def write_to_main_events_log(msg):
        data = {"msg": msg,
                "file": "clients",
                "sent_from": "ClientLib"
                }
        data = pickle.dumps(data)
        ClientLibrary.client_socket.sendto(data, common.MAIN_EVENTS_ADDR)

    @staticmethod
    def create_msg_id():
        return str(uuid.uuid4())

    @staticmethod
    def added_server_event(client_to_rm, new_sid, new_addr):
        log.info(f"ClientLib add server {new_sid} {new_addr}, rm client {client_to_rm}")
        ClientLibrary.servers[new_sid] = new_addr
        ClientLibrary.clients.pop(client_to_rm)

    @staticmethod
    def removed_server_event(server_to_rm, new_cid):
        log.info(f"ClientLib rm server {server_to_rm}, add client {new_cid}")
        ClientLibrary.clients[new_cid] = Client(new_cid, [])
        ClientLibrary.servers.pop(server_to_rm)

    @staticmethod
    def udp_receive(server_socket: socket.socket):
        while True:
            data, addr = server_socket.recvfrom(1024 * 4)

            data = pickle.loads(data)
            args = data['args']
            log.debug(f"ClientLib received {data} from {addr} | args: {args}")

            try:
                func = getattr(ClientLibrary, data['func'])
                log.debug(f"calling ClientLib.{data['func']} with {args}")
                if type(args) is dict:
                    func(**args)
                elif type(args) is list or type(args) is tuple:
                    func(*args)
                else:
                    func(args)
            except Exception as e:
                log.error(f"Error in ClientLib udp_receive: {e} ", exc_info=True)


class Client:
    def __init__(self, id, tokens: List[Token] = None):
        self.id = id
        self.tokens = tokens
        self.library = ClientLibrary

    def receive_message(self, msg):
        log.info(f"! client {self.id} received: {msg}")

    def pay(self, token_id: int, version: int, new_owner: str):
        """
        transfer token to another client.
        client gets "ok" response, new owner doesn't get a response.
        """
        log.info(f"client {self.id} request pay token{token_id} to {new_owner}")

        ClientLibrary.pay(self.id, token_id, version, new_owner)

    def get_tokens(self, owner_id: str):
        """
        returns tokens owned by owner.
        """
        log.info(f"client {self.id} request get_tokens({owner_id})")
        ClientLibrary.get_tokens(self.id, owner_id)
