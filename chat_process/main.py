import socket
import config

incoming_sockets = [None] * len(config.config['hosts'])

out_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
out_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

def init_sockets():
    for index, host in enumerate(config.config['hosts']):
        # initialize incoming sockets
        ip = host[0]
        port = host[1]
        out_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        out_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        out_sock.bind((ip, port))
        incoming_sockets[index] = out_sock

def unicast_send(destination, message):
    ''' destination: integer process ID
        message: string message '''
    host = config.config['hosts'][destination]
    ip = host[0]
    port = host[1]
    out_sock.sendto(message.encode('utf-8'), (ip, port))

def unicast_receive(source):
    ''' source: integer process ID
        return: message string '''
    data, addr = incoming_sockets[source].recvfrom(1024)
    return data

def multicast(message):
    ''' unicast the message to all known clients '''
    for id, host in enumerate(config.config['hosts']):
        unicast_send(id, message)

def deliver(source):
    ''' source: source process id
        return: incoming message from the source process '''
    return unicast_receive(source)


def run():
    init_sockets()

if __name__ == '__main__':
    run()