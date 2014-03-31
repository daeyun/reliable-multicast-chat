from queue import PriorityQueue
import random
import socket
import config
import struct
import threading
import sys
import time
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/../')
from helpers.unicast_helper import pack_message, unpack_message
from helpers.unicast_helper import stringify_vector_timestamp, parse_vector_timestamp


class Main:
    my_ID = 0
    sock = None
    message_size = 2048
    message_id = 0
    has_received = {}
    has_acknowledged = {}
    unack_messages = []
    mutex = threading.Lock()
    queue = PriorityQueue()
    timestamp = [0] * len(config.config['hosts'])
    holdback_queue = []

    def init_socket(self, id):
        host = config.config['hosts'][id]
        ip = host[0]
        port = host[1]

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock.bind((ip, port))
        self.sock.settimeout(0.01)

    def unicast_send(self, destination, message, msg_id = -1, is_ack = False, msg_timestamp = []):
        ''' destination: integer process ID
            message: string message '''
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

        message = pack_message([self.my_ID, id, is_ack, stringify_vector_timestamp(msg_timestamp), message])

        delay_time = random.uniform(0, 2 * self.delay_time)
        end_time = time.time() + delay_time
        self.queue.put((end_time, message.encode("utf-8"), ip, port))

    def unicast_receive(self, source):
        ''' source: integer process ID
            return: message string '''
        data, _ = self.sock.recvfrom(self.message_size)

        sender, message_id, is_ack, vector_str, message = unpack_message(data)

        sender = int(sender)
        message_id = int(message_id)

        message_timestamp = parse_vector_timestamp(vector_str)

        if is_ack == 'True':
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
        ''' unicast the message to all known clients '''
        for id, host in enumerate(config.config['hosts']):
            self.unicast_send(id, message)

    def deliver(self, sender, message):
        ''' source: source process id
            return: incoming message from the source process '''
        print(sender, "says: ", message)

    def process_message_queue(self):
        while True:
            (end_time, message, ip, port) = self.queue.get(block=True)
            if end_time <= time.time():
                self.sock.sendto(message, (ip, port))
            else:
                self.queue.put((end_time, message, ip, port))
                time.sleep(0.015)


    def process_ack(self):
        while True:
            time.sleep(0.1) # 100 msec

            with self.mutex:
                new_unack_messages = []
                for dest_id, message_id, message, message_timestamp in self.unack_messages:
                    if (dest_id, message_id) not in self.has_acknowledged:
                        new_unack_messages.append((dest_id, message_id, message, message_timestamp))
                        self.unicast_send(dest_id, message, message_id, msg_timestamp=message_timestamp)

                self.unack_messages = new_unack_messages


    def process_message_out(self):
        for line in sys.stdin:
            line = line[:-1]
            self.timestamp[self.my_ID] = self.timestamp[self.my_ID] + 1
            self.multicast(line)

    def process_message_in(self):
        while True:
            try:
                sender, message = self.unicast_receive(self.my_ID)
                if message is None:
                    continue
            except socket.timeout:
                pass
            except BlockingIOError:
                pass

    def run(self):
        if len(sys.argv) != 4:
            print('Usage: {} [process ID] [delay time] [drop rate]'.format(sys.argv[0]))
            return

        self.my_ID = int(sys.argv[1])
        self.delay_time = float(sys.argv[2])
        self.drop_rate = float(sys.argv[3])

        self.init_socket(self.my_ID)

        # multithreading to process sending/receiving messages
        out_thread = threading.Thread(target=self.process_message_out)
        out_thread.daemon = True

        in_thread = threading.Thread(target=self.process_message_in)
        in_thread.daemon = True

        ack_thread = threading.Thread(target=self.process_ack)
        ack_thread.daemon = True

        message_queue_thread = threading.Thread(target=self.process_message_queue)
        message_queue_thread.daemon = True

        message_queue_thread.start()
        ack_thread.start()
        out_thread.start()
        in_thread.start()

        out_thread.join()
        in_thread.join()
        ack_thread.join()
        message_queue_thread.join()


if __name__ == '__main__':
    chat_process = Main()
    chat_process.run()