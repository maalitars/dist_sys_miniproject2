#!/usr/bin/env python
import math
import random
import sys
import time

from node import Node


def start(nr_of_nodes):
    nr_of_nodes = int(nr_of_nodes)
    sockets = []
    port = 8001

    for i in range(1, nr_of_nodes+1):
        if i == 1:
            socket = Node(port, i, 'primary', 'NF', '')
        else:
            socket = Node(port, i, 'secondary', 'NF', '')
        port += 1
        socket.start()
        sockets.append(socket)

    sockets = sorted(sockets, key=lambda node: node.id)
    waitCommand = 1
    while waitCommand:
        command = input('Enter command(Exit command to quit):').lower()
        command_parts = command.split()
        if len(command_parts) > 1:
            # actual-order command handling
            if command_parts[0] == "actual-order":
                order = command_parts[1]
                # creating connections between the nodes
                for node in sorted(sockets, key=lambda node: node.id):
                    node.votes.clear()
                    if node.role != 'primary':
                        for remote_node in sorted(sockets, key=lambda node: node.id):
                            if remote_node.role != 'primary' and node.port != remote_node.port:
                                new_connection = node.connect_with_node(remote_node.port)
                                if node.state == 'F':
                                    proxy_order = random.choice(
                                        ['attack', 'retreat'])  # if node is faulty, we get a random order from it
                                    new_connection.send(('actual-order-send:%s' % proxy_order).encode('utf-8'))
                                else:
                                    new_connection.send(('actual-order-send:%s' % order).encode('utf-8'))
                    else:
                        node.majority = order
                time.sleep(0.01)
                # handling the final decision making (whether to attack, retreat or is the decision undefined)
                nr_of_faulty_nodes = 0
                for node in sorted(sockets, key=lambda node: node.id):
                    if node.state == 'F':
                        nr_of_faulty_nodes += 1

                # easy decision to make if there are no faulty nodes
                if nr_of_faulty_nodes == 0:
                    majority_vote = ''
                    nr_of_maj_votes = 0
                    for node in sorted(sockets, key=lambda node: node.id):
                        if node.role != 'primary':
                            node.votes.append(order)
                            majority_vote = max(set(node.votes), key = node.votes.count)
                            node.majority = majority_vote
                            nr_of_maj_votes += 1
                    execute_message = 'Execute order: ' + majority_vote + '! Non-faulty nodes in the system – ' + str(nr_of_maj_votes) + ' out of ' + str(len(sockets)) + ' quorum suggest ' + str(majority_vote)

                # making the decision when there is at least one faulty node
                else:
                    is_undefined = False
                    majority_vote = ''
                    nr_of_maj_votes = 0
                    for node in sorted(sockets, key=lambda node: node.id):
                        if node.role != 'primary':
                            # if we don't have enough non-faulty nodes in the system, then we cannot make a decision
                            if len(node.votes) + 1 < (3*nr_of_faulty_nodes + 1):
                                node.majority = 'undefined'
                                is_undefined = True
                            else:
                                node.votes.append(order)
                                node.majority = max(set(node.votes), key = node.votes.count)
                                majority_vote = node.majority
                                if node.majority == order:
                                    nr_of_maj_votes += 1
                    if is_undefined:
                        execute_message = 'Execute order: cannot be determined – not enough generals in the system! ' + str(nr_of_faulty_nodes) + ' faulty nodes in the system – ' + str(len(sockets) - 1) + ' out of ' + str(len(sockets)) + ' quorum not consistent'
                    else:
                        execute_message = 'Execute order: ' + majority_vote + '! ' + str(nr_of_faulty_nodes) + ' faulty nodes in the system – ' + str(nr_of_maj_votes) + ' out of ' + str(len(sockets)) + ' quorum suggest ' + str(majority_vote)
                for node in sorted(sockets, key=lambda node: node.id):
                    node.votes.clear()
                    print(str(node.name_) + ', ' + str(node.role) + ', majority=' + str(node.majority) + ', state=' + str(node.state))
                print(execute_message)

            # g-state command handling with arguments
            elif command_parts[0] == "g-state":
                general_id = int(command_parts[1])
                general_state = command_parts[2]
                for node in sorted(sockets, key=lambda node: node.id):
                    if node.id == general_id:
                        if general_state == 'faulty':
                            node.state = 'F'
                        elif general_state == 'non-faulty':
                            node.state = 'NF'

            # g-kill command handling
            elif command_parts[0] == "g-kill":
                general_id = int(command_parts[1])
                sockets = sorted(sockets, key=lambda node: node.id)
                ix = -1
                for i, socket in enumerate(sockets):
                    if socket.id == general_id:
                        ix = i
                if sockets[ix].role == 'primary':
                    sockets.pop(ix)
                    lowest_id = math.inf
                    lowest_ix = -1
                    for i, socket in enumerate(sockets):
                        if socket.id < lowest_id:
                            lowest_id = socket.id
                            lowest_ix = i
                    sockets[lowest_ix].role = 'primary'
                else:
                    sockets.pop(ix)

            # g-add command handling
            elif command_parts[0] == "g-add":
                nr_of_new_gens = int(command_parts[1])
                highest_id = -1
                highest_port = -1
                for node in sorted(sockets, key=lambda node: node.id):
                    if node.id > highest_id:
                        highest_id = node.id
                    if node.port > highest_port:
                        highest_port = node.port
                for new_id, new_port in zip(range(highest_id+1, highest_id+1+nr_of_new_gens), range(highest_port+1, highest_port+1+nr_of_new_gens)):
                    socket = Node(new_port, new_id, 'secondary', 'NF', '')
                    socket.start()
                    sockets.append(socket)

        # g-state command handling
        elif command == "g-state":
            for node in sorted(sockets, key=lambda node: node.id):
                print(str(node.name_) + ',' + str(node.role) + ',state=' + str(node.state))
        elif command == "exit":
            print("Program exit.")
            sys.exit()


if len(sys.argv) != 2:
    print("Incorrect arguments!")
else:
    start(sys.argv[1])