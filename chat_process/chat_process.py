from queue import PriorityQueue
import random
import socket
import config
import threading
import sys
import time
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/../')
from helpers.unicast_helper import pack_message, unpack_message
from helpers.unicast_helper import stringify_vector_timestamp, parse_vector_timestamp


class ChatProcess:
    def __init__(self, process_id, delay_time, drop_rate, num_processes):
        self.my_id = process_id
        self.delay_time = delay_time
        self.drop_rate = drop_rate

        self.message_size = 2048
        self.message_id = 0
        self.has_received = {}
        self.has_acknowledged = {}
        self.unack_messages = []
        self.holdback_queue = []

        self.queue = PriorityQueue()
        self.mutex = threading.Lock()

        self.timestamp = [0] * num_processes

        self.init_socket(process_id)

    def init_socket(self, id):
        """ Initialize the UDP socket. """
        host = config.config['hosts'][id]
        ip = host[0]
        port = host[1]

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock.bind((ip, port))
        self.sock.settimeout(0.01)

    def unicast_send(self, destination, message, msg_id = -1, is_ack = False, msg_timestamp = []):
        """ Push an outgoing message to the message queue. """
        host = config.config['hosts'][destination]
        ip = host[0]
        port = host[1]

        if len(msg_timestamp) == 0:
            msg_timestamp = self.timestamp[:]

        id = None
        if not is_ack:
            if msg_id == -1:
                self.message_id = self.message_id + 1
                id = self.message_id
                with self.mutex:
                    self.unack_messages.append((destination, id, message, msg_timestamp[:]))
            else:
                id = msg_id
        else:
            id = msg_id

        if random.random() <= self.drop_rate:
            return

        message = pack_message([self.my_id, id, is_ack, stringify_vector_timestamp(msg_timestamp), message])

        delay_time = random.uniform(0, 2 * self.delay_time)
        end_time = time.time() + delay_time
        self.queue.put((end_time, message.encode("utf-8"), ip, port))

    def unicast_receive(self, source):
        """ Receive UDP messages from other chat processes and store them in the holdback queue. """
        data, _ = self.sock.recvfrom(self.message_size)
        [sender, message_id, is_ack, message_timestamp, message] = unpack_message(data)

        if is_ack:
            self.has_acknowledged[(sender, message_id)] = True
            return (sender, None)
        else:
            # send acknowledgement to the sender
            self.unicast_send(int(sender), "", message_id, True)

            if (sender, message_id) not in self.has_received:
                self.has_received[(sender, message_id)] = True
                self.holdback_queue.append((sender, message_timestamp[:], message))
                self.update_holdback_queue()
                return (sender, message)
            else:
                return (sender, None)

    def update_holdback_queue(self):
        """ Compare message timestamps to ensure casual ordering. """
        while True:
            new_holdback_queue = []
            removed = []
            for sender, v, message in self.holdback_queue:
                should_remove = True
                for i in range(len(v)):
                    if i == sender:
                        if v[i] != self.timestamp[i] + 1:
                            should_remove = False
                    else:
                        if v[i] > self.timestamp[i]:
                            should_remove = False
                if not should_remove:
                    new_holdback_queue.append((sender, v, message))
                else:
                    removed.append((sender, v, message))

            for sender, v, message in removed:
                self.timestamp[sender] = self.timestamp[sender] + 1
                self.deliver(sender, message)

            self.holdback_queue = new_holdback_queue

            if len(removed) == 0:
                break

    def multicast(self, message):
        """ Unicast the message to all known clients. """
        for id, host in enumerate(config.config['hosts']):
            self.unicast_send(id, message)

    def deliver(self, sender, message):
        """ Do something with the received message. """
        print(sender, "says: ", message)

    def message_queue_handler(self):
        """ Thread that actually sends out messages when send time <= current_time. """
        while True:
            (send_time, message, ip, port) = self.queue.get(block=True)
            if send_time <= time.time():
                self.sock.sendto(message, (ip, port))
            else:
                self.queue.put((send_time, message, ip, port))
                time.sleep(0.01)

    def ack_handler(self):
        """ Thread that re-sends all unacknowledged messages. """
        while True:
            time.sleep(0.1)

            with self.mutex:
                new_unack_messages = []
                for dest_id, message_id, message, message_timestamp in self.unack_messages:
                    if (dest_id, message_id) not in self.has_acknowledged:
                        new_unack_messages.append((dest_id, message_id, message, message_timestamp))
                        self.unicast_send(dest_id, message, message_id, msg_timestamp=message_timestamp)
                self.unack_messages = new_unack_messages


    def user_input_handler(self):
        """ Thread that waits for user input and multicasts to other processes. """
        for line in sys.stdin:
            line = line[:-1]
            self.timestamp[self.my_id] = self.timestamp[self.my_id] + 1
            self.multicast(line)

    def incoming_message_handler(self):
        """ Thread that listens for incoming UDP messages """
        while True:
            try:
                sender, message = self.unicast_receive(self.my_id)
                if message is None:
                    continue
            except socket.timeout:
                pass
            except BlockingIOError:
                pass

    def run(self):
        """ Initialize and start all threads. """
        callable_objects = [
            self.ack_handler,
            self.message_queue_handler,
            self.incoming_message_handler,
            self.user_input_handler,
            ]

        threads = []
        for callable in callable_objects:
            thread = threading.Thread(target=callable)
            thread.daemon = True
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
