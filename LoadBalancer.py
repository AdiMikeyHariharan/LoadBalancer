import sys
import socket
import select
import random


##########################################
''''RUNNING MANUAL
    STEP 1 : START THE LOAD BALANCER
    STEP 2 : MAKE SURE THE SERVER IS RUNNING
    STEP 3 : CONNECT TO THE LOAD BALANCER AND SEND REQUESTS OR DATA
'''
###########################################
SERVER_POOL = [('127.0.0.1', 7777)]


class LoadBalancer:
    flow_map = dict()
    socket_list = list()

    def __init__(self, ip, port, algorithm="round robin"):
        self.ip = ip
        self.port = port
        self.algorithm = algorithm

        self.client_side_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_side_socket.bind((self.ip, self.port))
        self.client_side_socket.listen()
        self.socket_list.append(self.client_side_socket)

    def start(self):
        while True:
            readable, writable, error = select.select(self.socket_list, [], [])
            for sock in readable:
                if sock == self.client_side_socket:
                    # As the client_side_socket is readable, it means that it can accept connections
                    print('*' * 30 + 'flow start' + 30 * '*')
                    self.conn_accept()
                else:
                    try:
                        data = sock.recv(1024)
                        if data:
                            self.data_recv(sock, data)
                        else:
                            self.conn_close(sock)
                    except:
                        self.conn_close(sock)

    def conn_accept(self):
        client_socket, client_addr = self.client_side_socket.accept()
        print(f'Client connected {client_addr} <=========> {self.client_side_socket.getsockname()}')
        server_ip, server_port = self.select_server(SERVER_POOL, self.algorithm)
        print(f'{server_ip} {server_port}')
        server_side_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server_side_socket.connect((server_ip, server_port))
            print(f'Server side socket {server_side_socket.getsockname()} Initiated')
            print(f'Server Connected {server_side_socket.getsockname()} <======> {server_ip}:{server_port}')
        except Exception as e:
            print(f'Cannot connect with remote server err: {e}')
            print(f'Closing connection with the client {client_addr}')
            client_socket.close()
            return
        self.flow_map[client_socket] = server_side_socket
        self.flow_map[server_side_socket] = client_socket
        self.socket_list.append(server_side_socket)
        self.socket_list.append(client_socket)

    def data_recv(self, sock, data):
        print(f'Receiving packets {sock.getpeername()} <=====> {sock.getsockname()} and data: {data}')
        remote_socket = self.flow_map[sock]
        remote_socket.send(data)
        print(f'Sending packets {remote_socket.getsockname()} <======> {remote_socket.getpeername()} and data: {data}')

    def conn_close(self, sock):
        print(f'Client {sock.getpeername()} has disconnected')
        print('*' * 30 + 'flow end' + 30 * '*')

        ss_socket = self.flow_map[sock]
        self.socket_list.remove(sock)
        self.socket_list.remove(ss_socket)
        sock.close()
        ss_socket.close()

        del self.flow_map[sock]
        del self.flow_map[ss_socket]

    def select_server(self, servers, algorithm):
        if algorithm == "random":
            return random.choice(servers)


if __name__ == "__main__":
    try:
        LoadBalancer('localhost', 5555, 'random').start()
    except KeyboardInterrupt:
        print("Ctrl - C - Exit Load Balancer")
        sys.exit(1)


