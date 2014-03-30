import socket
import config
import struct
import threading
import sys


class Main:
    my_ID = 0
    sock = None
    message_size = 2048
    message_id = 0
    has_received = {}

    def init_socket(self, id):
        host = config.config['hosts'][id]
        ip = host[0]
        port = host[1]

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock.bind((ip, port))
        self.sock.settimeout(0.01)

    def unicast_send(self, destination, message, is_ack = False, ack_message_id = 0):
        ''' destination: integer process ID
            message: string message '''
        host = config.config['hosts'][destination]
        ip = host[0]
        port = host[1]

        id = None
        if not is_ack:
            self.message_id = self.message_id + 1
            id = self.message_id
        else:
            id = ack_message_id

        message = str(self.my_ID) + "," + str(id) + "," + str(is_ack) + "," + message
        self.sock.sendto(message.encode("utf-8"), (ip, port))

    def unicast_receive(self, source):
        ''' source: integer process ID
            return: message string '''
        data, addr = self.sock.recvfrom(self.message_size)
        decoded_data = data.decode('utf-8')

        sender, message_id, is_ack, message = decoded_data.split(',', 3)

        if bool(is_ack):
           
           return (sender, None)
        else:
            # send acknowledgement to the sender
            self.unicast_send(int(sender), "", True, message_id)

            if (sender, message_id) in self.has_received:
                self.has_received[(sender, message_id)] = True
                return (sender, message)
            else:
                return (sender, None)

    def multicast(self, message):
        ''' unicast the message to all known clients '''
        for id, host in enumerate(config.config['hosts']):
            self.unicast_send(id, message)

    def deliver(self, source):
        ''' source: source process id
            return: incoming message from the source process '''
        return self.unicast_receive(source)

    def process_message_out(self):
        for line in sys.stdin:
            line = line[:-1]
            self.multicast(line)


    def process_message_in(self):
        while True:
            try:
                sender, message = self.deliver(self.my_ID)
                if message == None:
                    continue
                print(sender, "says: ", message)
            except socket.timeout:
                pass
            except BlockingIOError:
                pass

    def run(self):
        if len(sys.argv) != 2:
            print('Usage: {} [process ID]'.format(sys.argv[0]))
            return

        self.my_ID = int(sys.argv[1])

        self.init_socket(self.my_ID)

        # multithreading to process sending/receiving messages
        out_thread = threading.Thread(target=self.process_message_out)
        out_thread.daemon = True

        in_thread = threading.Thread(target=self.process_message_in)
        in_thread.daemon = True

        out_thread.start()
        in_thread.start()

        out_thread.join()
        in_thread.join()


if __name__ == '__main__':
    chat_process = Main()
    chat_process.run()