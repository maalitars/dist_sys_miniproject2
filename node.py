#!/usr/bin/env python
import socket
import time
import threading

from nodeconnection import NodeConnection

class Node(threading.Thread):
    def __init__(self, port, id, role, state, majority):
        super(Node, self).__init__()

        self.terminate_flag = threading.Event()

        self.host = "127.0.0.1"
        self.port = port

        self.callback = self.node_callback

        self.id = id
        self.name_ = 'G' + str(self.id)
        self.role = role
        self.state = state
        self.majority = majority
        self.votes = []

        # Nodes that have established a connection with this node
        self.nodes_inbound = []  # Nodes that are connect with us N->(US)

        # Nodes that this nodes is connected to
        self.nodes_outbound = []  # Nodes that we are connected to (US)->N

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.init_server()

    def node_callback(self, event, main_node, connected_node, data):
        if event == 'connect':
            self.connect_with_node(int(data))
        elif event == 'actual-order-send':
            connected_node.send(("actual-order-receive:%s" % str(data)).encode("utf-8"))
            # send data to all the secondary nodes in the connection variable
        elif event == 'actual-order-receive':
            if self.role != 'primary':
                vote = str(data)
                self.votes.append(vote)

    def init_server(self):
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.settimeout(10.0)
        self.sock.listen(1)

    @property
    def all_nodes(self):
        """Return a list of all the nodes, inbound and outbound, that are connected with this node."""
        return self.connections

    def connect_with_node(self, port):
        if port == self.port:
            print("connect_with_node: Cannot connect with yourself!!")
            return False

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('127.0.0.1', port))

            sock.send(str(self.id).encode('utf-8'))
            connected_node_id = sock.recv(4096).decode('utf-8')

            # create new NodeConnection
            thread_client = self.create_new_connection(sock, connected_node_id, '127.0.0.1', port)
            thread_client.start()

            return thread_client

        except Exception as e:
            pass

    def create_new_connection(self, sock, connected_node_id, host, port):
        return NodeConnection(self, sock, connected_node_id, host, port)

    def node_message(self, node, command, data=''):
        # This method is invoked when a node sends a message
        if self.callback is not None:
            self.callback(command, self, node, data)

    def stop(self):
        self.terminate_flag.set()

    def run(self):
        # Check whether the thread needs to be closed
        while not self.terminate_flag.is_set():
            try:
                connection, client_address = self.sock.accept()

                connected_node_id = connection.recv(4096).decode('utf-8')
                connection.send(str(self.id).encode('utf-8'))

                thread_client = self.create_new_connection(connection, connected_node_id, client_address[0],
                                                           client_address[1])
                thread_client.start()


            except socket.timeout:
                pass
            except Exception as e:
                raise e

            time.sleep(0.01)
        for connec in self.connections:
            connec.stop()

        self.sock.settimeout(None)
        self.sock.close()

    def __str__(self):
        return 'Node: {}:{}'.format(self.host, self.port)

    def __repr__(self):
        return '<Node {}:{} id: {}>'.format(self.host, self.port, self.id)