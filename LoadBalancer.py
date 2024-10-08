import sys
import socket
import select
import random
import hashlib
import psutil  # For dynamic CPU and memory usage
from collections import defaultdict
import logging
import time
import threading
import json
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

##########################################
'''
RUNNING MANUAL
    STEP 1 : START THE LOAD BALANCER
    STEP 2 : MAKE SURE THE SERVER IS RUNNING
    STEP 3 : CONNECT TO THE LOAD BALANCER AND SEND REQUESTS OR DATA
'''
###########################################
DEFAULT_SERVER_POOL = [('127.0.0.1', 7777), ('127.0.0.1', 8888), ('127.0.0.1', 9999)]
DEFAULT_SERVER_WEIGHTS = {('127.0.0.1', 7777): 2, ('127.0.0.1', 8888): 1, ('127.0.0.1', 9999): 1}
ACTIVE_CONNECTIONS = defaultdict(int)
OVERLOAD_THRESHOLD = 5  # Max connections before a server is considered overloaded
REQUEST_LIMIT_PER_IP = 100  # Limit requests from a single IP address
CONNECTION_TIMEOUT = 60  # Timeout for idle connections (in seconds)

# Store IP request counts and timestamps for rate limiting
ip_request_count = defaultdict(lambda: {"count": 0, "timestamp": time.time()})

def least_connections():
    return min(DEFAULT_SERVER_POOL, key=lambda server: ACTIVE_CONNECTIONS[server])

def round_robin():
    round_robin.counter = (round_robin.counter + 1) % len(DEFAULT_SERVER_POOL)
    return DEFAULT_SERVER_POOL[round_robin.counter]

# Initialize round robin counter
round_robin.counter = -1

def ip_hashing(client_ip):
    hashed_ip = int(hashlib.sha256(client_ip.encode()).hexdigest(), 16)
    return DEFAULT_SERVER_POOL[hashed_ip % len(DEFAULT_SERVER_POOL)]

def weighted_round_robin(weights):
    total_weight = sum(weights.values())
    cumulative_weights = []
    cumulative_sum = 0

    for server, weight in weights.items():
        cumulative_sum += weight
        cumulative_weights.append((server, cumulative_sum))

    random_choice = random.uniform(0, total_weight)
    for server, cumulative_weight in cumulative_weights:
        if random_choice <= cumulative_weight:
            return server

class LoadBalancer:
    flow_map = dict()
    socket_list = list()
    statistics = {"total_requests": 0, "total_response_time": 0}
    health_check_log = []

    def __init__(self, ip, port, config_file=None):
        self.ip = ip
        self.port = port
        self.algorithm = "weighted round robin"
        self.weights = DEFAULT_SERVER_WEIGHTS.copy()

        # Load configuration if a config file is provided
        if config_file:
            self.load_config(config_file)
        else:
            self.SERVER_POOL = DEFAULT_SERVER_POOL

        self.client_side_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_side_socket.bind((self.ip, self.port))
        self.client_side_socket.listen()
        self.socket_list.append(self.client_side_socket)

        logging.info(f"LoadBalancer initialized on {self.ip}:{self.port} with algorithm '{self.algorithm}'")
        
        # Start a separate thread for health checks
        self.running = True
        self.health_check_interval = 30  # seconds
        self.health_check_thread = threading.Thread(target=self.start_health_check)
        self.health_check_thread.start()

    def load_config(self, config_file):
        """Load server configurations from a JSON file."""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
                self.SERVER_POOL = config.get('servers', DEFAULT_SERVER_POOL)
                self.weights = config.get('weights', DEFAULT_SERVER_WEIGHTS)
                logging.info("Configuration loaded from JSON.")
        except Exception as e:
            logging.error(f"Error loading configuration: {e}")

    def start_health_check(self):
        while self.running:
            self.check_health()
            time.sleep(self.health_check_interval)

    def start(self):
        logging.info("LoadBalancer is now running.")
        while True:
            readable, writable, error = select.select(self.socket_list, [], [])
            for sock in readable:
                if sock == self.client_side_socket:
                    # Accept new client connections
                    self.conn_accept()
                else:
                    # Handle incoming data from clients or servers
                    try:
                        data = sock.recv(4096)  # Increased buffer size for requests
                        if data:
                            self.data_recv(sock, data)
                        else:
                            self.conn_close(sock)
                    except Exception as e:
                        logging.error(f"Error receiving data: {e}")
                        self.conn_close(sock)

    def conn_accept(self):
        client_socket, client_addr = self.client_side_socket.accept()
        logging.info(f'Client connected {client_addr} <=========> {self.client_side_socket.getsockname()}')
        
        # Check rate limit for the client IP
        if self.rate_limit_exceeded(client_addr[0]):
            logging.warning(f'Rate limit exceeded for IP: {client_addr[0]}')
            client_socket.close()
            return
        
        server_ip, server_port = self.select_server(self.SERVER_POOL, self.algorithm, client_addr[0])
        logging.info(f'Forwarding to server {server_ip}:{server_port}')
        server_side_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server_side_socket.connect((server_ip, server_port))
            logging.info(f'Server side socket {server_side_socket.getsockname()} Initiated')
            logging.info(f'Server Connected {server_side_socket.getsockname()} <======> {server_ip}:{server_port}')
            ACTIVE_CONNECTIONS[(server_ip, server_port)] += 1
        except Exception as e:
            logging.error(f'Cannot connect with remote server err: {e}')
            logging.info(f'Closing connection with the client {client_addr}')
            client_socket.close()
            return
        
        # Update the flow map
        self.flow_map[client_socket] = server_side_socket
        self.flow_map[server_side_socket] = client_socket
        self.socket_list.append(server_side_socket)
        self.socket_list.append(client_socket)

    def data_recv(self, sock, data):
        logging.info(f'Receiving packets {sock.getpeername()} <=====> {sock.getsockname()} and data: {data}')
        remote_socket = self.flow_map[sock]

        start_time = time.time()  # Start timing for response
        try:
            # Log the request
            logging.info(f'Forwarding request to {remote_socket.getpeername()}')
            remote_socket.send(data)
            logging.info(f'Sent data to server {remote_socket.getpeername()}')

            # Receive the response from the server and send it back to the client
            response = remote_socket.recv(4096)  # Increased buffer size for responses
            logging.info(f'Received response from server {remote_socket.getpeername()} and sending back to client {sock.getpeername()}')
            sock.send(response)
            logging.info(f'Sent response back to client {sock.getpeername()}')

            # Update statistics
            response_time = time.time() - start_time
            self.statistics['total_requests'] += 1
            self.statistics['total_response_time'] += response_time
            logging.info(f'Response time: {response_time:.4f} seconds')
        except Exception as e:
            logging.error(f'Error during data transfer: {e}')
            self.conn_close(sock)

    def conn_close(self, sock):
        logging.info(f'Client {sock.getpeername()} has disconnected')
        logging.info('*' * 30 + ' flow end ' + 30 * '*')

        ss_socket = self.flow_map[sock]
        self.socket_list.remove(sock)
        self.socket_list.remove(ss_socket)
        ACTIVE_CONNECTIONS[(ss_socket.getpeername()[0], ss_socket.getpeername()[1])] -= 1
        sock.close()
        ss_socket.close()
        del self.flow_map[sock]
        del self.flow_map[ss_socket]

    def select_server(self, servers, algorithm, client_ip=None):
        if algorithm == "random":
            return random.choice(servers)
        if algorithm == "least connection":
            return least_connections()
        if algorithm == "round robin":
            return round_robin()
        if algorithm == "ip hashing" and client_ip:
            return ip_hashing(client_ip)
        if algorithm == "weighted round robin":
            return weighted_round_robin(self.weights)
        if algorithm == "cpu based":
            return self.cpu_based_balancing()
        if algorithm == "memory based":
            return self.memory_based_balancing()

    def cpu_based_balancing(self):
        # Return the server with the lowest CPU usage
        server_cpu_usage = [(server, psutil.cpu_percent(interval=1)) for server in self.SERVER_POOL]
        logging.info(f'CPU usage: {server_cpu_usage}')
        return min(server_cpu_usage, key=lambda x: x[1])[0]

    def memory_based_balancing(self):
        # Return the server with the lowest memory usage
        server_memory_usage = [(server, psutil.virtual_memory().percent) for server in self.SERVER_POOL]
        logging.info(f'Memory usage: {server_memory_usage}')
        return min(server_memory_usage, key=lambda x: x[1])[0]

    def check_health(self):
        # Check the health of servers in the pool
        for server in self.SERVER_POOL:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)  # Set a timeout for the connection attempt
            try:
                sock.connect(server)
                logging.info(f'Server {server} is healthy.')
            except socket.error:
                logging.warning(f'Server {server} is down!')
                self.remove_server(server)  # Remove overloaded server
            finally:
                sock.close()

        self.log_server_status()

    def log_server_status(self):
        logging.info(f'Active Connections: {ACTIVE_CONNECTIONS}')
        overloaded_servers = [server for server in self.SERVER_POOL if ACTIVE_CONNECTIONS[server] > OVERLOAD_THRESHOLD]
        if overloaded_servers:
            logging.warning(f'Overloaded servers: {overloaded_servers}')

    def rate_limit_exceeded(self, client_ip):
        current_time = time.time()
        ip_info = ip_request_count[client_ip]
        
        # Check if 60 seconds have passed since the last recorded request
        if current_time - ip_info["timestamp"] > 60:
            ip_info["count"] = 0  # Reset count after the time window
            ip_info["timestamp"] = current_time

        ip_info["count"] += 1
        return ip_info["count"] > REQUEST_LIMIT_PER_IP

    def remove_server(self, server):
        if server in self.SERVER_POOL:
            self.SERVER_POOL.remove(server)
            logging.info(f'Removed server {server} from pool due to overload.')

    def server_status(self):
        # Log the status of active connections
        logging.info(f'Active Connections: {ACTIVE_CONNECTIONS}')

    def reload_config(self, config_file):
        """Reload configuration from a JSON file."""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
                self.SERVER_POOL[:] = config.get('servers', self.SERVER_POOL)
                self.weights = config.get('weights', DEFAULT_SERVER_WEIGHTS)
                logging.info("Configuration reloaded.")
        except Exception as e:
            logging.error(f"Error reloading configuration: {e}")

    def change_algorithm(self, new_algorithm):
        """Change load balancing algorithm dynamically."""
        if new_algorithm in ["random", "least connection", "round robin", "ip hashing", "weighted round robin", "cpu based", "memory based"]:
            self.algorithm = new_algorithm
            logging.info(f"Load balancing algorithm changed to {self.algorithm}.")
        else:
            logging.error(f"Invalid algorithm: {new_algorithm}")

    def change_log_level(self, level):
        """Change logging level dynamically."""
        logging.getLogger().setLevel(level)
        logging.info(f"Log level changed to {level}")

    def shutdown(self):
        logging.info("Shutting down Load Balancer...")
        self.running = False
        for sock in self.socket_list:
            sock.close()
        logging.info("Load Balancer shutdown complete.")

    def persist_statistics(self):
        """Persist statistics to a JSON file."""
        try:
            with open('load_balancer_stats.json', 'w') as f:
                json.dump(self.statistics, f)
                logging.info("Statistics saved to load_balancer_stats.json.")
        except Exception as e:
            logging.error(f"Error saving statistics: {e}")

if __name__ == "__main__":
    try:
        # Load balancer can now be initialized with a configuration file
        lb = LoadBalancer('localhost', 5555, config_file='config.json')  # Example config file
        lb.check_health()  # Check server health at startup
        lb.start()
    except KeyboardInterrupt:
        logging.info("Ctrl - C - Exit Load Balancer")
        lb.shutdown()
        lb.persist_statistics()  # Persist statistics before shutdown
        sys.exit(1)
