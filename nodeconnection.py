#!/usr/bin/env python
import socket
import time
import threading
import json

"""
We used an example p2p network as a starting point to build the DHT: https://github.com/macsnoeren/python-p2p-network
This class represents connections between two nodes.
With this a node can send a message to successor and successor can respond if needed.
Data is sent mostly through bytes with utf-8 encoding. 
"""


class NodeConnection(threading.Thread):

    def __init__(self, main_node, sock, id, host, port):

        super(NodeConnection, self).__init__(daemon=True)

        self.host = host
        self.port = port
        self.main_node = main_node
        self.sock = sock
        self.terminate_flag = threading.Event()

        # The id of the connected node
        self.id = int(id)

        # End of transmission character for the network streaming messages
        self.EOT_CHAR = 0x04.to_bytes(1, 'big')

        self.info = {}

    def send(self, data, encoding_type='utf-8'):
        if isinstance(data, str):
            self.sock.sendall(data.encode(encoding_type) + self.EOT_CHAR)

        elif isinstance(data, dict):
            try:
                json_data = json.dumps(data)
                json_data = json_data.encode(encoding_type) + self.EOT_CHAR
                self.sock.sendall(json_data)

            except Exception as e:
                print('Unexpected Error in send message')
                print(e)

        elif isinstance(data, bytes):
            bin_data = data + self.EOT_CHAR
            self.sock.sendall(bin_data)

        else:
            pass

    def stop(self):
        self.terminate_flag.set()

    def parse_packet(self, packet):
        try:
            packet_decoded = packet.decode('utf-8')

            #all communication comes in form "command:[data]" if data exists
            #otherwise, it is just "command"
            packet_decoded = packet_decoded.split(":")
            if len(packet_decoded) == 2:
                command = packet_decoded[0]
                data = packet_decoded[1]
                return command, data
            else:
                return packet_decoded[0], ''
        except UnicodeDecodeError:
            return packet

    def run(self):
        self.sock.settimeout(10.0)
        buffer = b''

        while not self.terminate_flag.is_set():
            chunk = b''

            try:
                chunk = self.sock.recv(4096)

            except socket.timeout:
                pass

            except Exception as e:
                self.terminate_flag.set()

            if chunk != b'':
                buffer += chunk
                eot_pos = buffer.find(self.EOT_CHAR)

                while eot_pos > 0:
                    packet = buffer[:eot_pos]
                    buffer = buffer[eot_pos + 1:]

                    command, data = self.parse_packet(packet)
                    #send message to successor
                    self.main_node.node_message(self, command, data)

                    eot_pos = buffer.find(self.EOT_CHAR)

            time.sleep(0.01)

        self.sock.settimeout(None)
        self.sock.close()

    def __str__(self):
        return 'NodeConnection: {}:{} <-> {}:{} ({})'.format(self.main_node.host, self.main_node.port, self.host,
                                                             self.port, self.id)

    def __repr__(self):
        return '<NodeConnection: Node {}:{} <-> Connection {}:{}>'.format(self.main_node.host, self.main_node.port,
                                                                          self.host, self.port)
