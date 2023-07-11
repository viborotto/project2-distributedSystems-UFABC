import socket, os, threading, random, pickle, threading

# Capturar IP e porta do servidor
server_ip = input("Defina o IP do Servidor (default: 127.0.0.1): ") or "127.0.0.1"
server_port = int(input("Defina a PORTA do Servidor(10097, 10098 e 10099): ") or "10097")

# Capturar IP e porta do líder
lider_ip = input("Defina o IP do Servidor LIDER (default: 127.0.0.1): ") or "127.0.0.1"
lider_port = int(input("Defina a PORTA do Servidor LIDER (10097, 10098 e 10099): ") or "10097")

# tabela de hash local para armazenar os pares chave-valor e os timestamps associados.
class HashTableKV:
    def __init__(self):
        self.store = {}
        self.timestamps = {}

    def put(self, key, value, timestamp):
        self.store[key] = value
        self.timestamps[key] = timestamp

    def get(self, key):
        value = self.store.get(key)
        timestamp = self.timestamps.get(key)
        return value, timestamp

    def search(self, key):
        return key in self.store

    def update(self, key, new_value, new_timestamp):
        if key in self.store:
            self.store[key] = new_value
            self.timestamps[key] = new_timestamp
        else:
            raise KeyError("Key does not exist in KeyValueStore")
def processarMensagem(client_socket, address, data, key_value_store, leader_ip, leader_port):
    # Implemente aqui a lógica para processar a requisição recebida do cliente
    # Se o servidor não for o líder, encaminhe a requisição para o líder
    # Caso contrário, se o comando for PUT, insira a chave-valor na tabela de hash local com o timestamp atual
    # Se o comando for GET, obtenha o valor e o timestamp associados à chave

    # Caso seja Put e NAO SEJA LIDER
    if data.startswith('PUT') and (leader_ip != address[0] or leader_port != address[1]):
        # REDIRECIONA A REQUISICAO PARA O LIDER
        print("NAO SOU O SERVIDOR LIDER")
        print("REDIRECIONANDO REQUEST PARA O LIDER")
        response = forward_request_to_leader(leader_ip, leader_port, data)
    else:
        print("SOU O SERVIDOR LIDER")
        response = "OK"  # Exemplo de resposta para o cliente
        if data.startswith('PUT'):
            key_value = data[4:]  # Extrair a parte do comando após "PUT "
            key, value = key_value.split('=')
            timestamp = key_value_store.timestamps.get(key, 0) + 1
            # O VERIFICAR SE A KEY JA EXISTE NO HASHTABLE
            if key_value_store.search():
                # SE EXISTIR: ATUALIZA O VALOR E TIMESTAMP
                print("chave ja existe")
                key_value_store.update(key, value, timestamp)
                print("atualizado o valor e timestamp da chave: " + key)
            else:
                # INSERE KEY E VALUE EM UMA TABELA DE HASHLOCAL
                key_value_store.put(key, value, timestamp)
            # TODO: 2 - REPLICAR OS DADOS NOS OUTROS SERVIDORES(K, V, Timestamp)
                # Mensagem: REPLICATION key value timestamp
        elif data.startswith('GET'):
            key = data[4:]  # Extrair a parte do comando após "GET "
            value, timestamp = key_value_store.get(key)
            response = f"Value: {value}, Timestamp: {timestamp}"

    client_socket.sendall(response.encode())

def forward_request_to_leader(leader_ip, leader_port, data):
    leader_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        leader_socket.connect((leader_ip, leader_port))
        leader_socket.sendall(data.encode())
        response = leader_socket.recv(1024).decode()
        return response
    finally:
        leader_socket.close()

def replicate_to_server(server_ip, server_port, replication_data):
    replication_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        replication_socket.connect((server_ip, server_port))
        replication_socket.sendall(replication_data.encode())
        response = replication_socket.recv(1024).decode()
        print(f"Replication response from {server_ip}:{server_port}: {response}")
    finally:
        replication_socket.close()

def replicate_to_servers(key, value, timestamp, leader_ip, leader_port):
    servers = [
        ("127.0.0.1", 10098),  # Endereço do servidor 2
        ("127.0.0.1", 10099)   # Endereço do servidor 3
    ]

    replication_data = f"REPLICATION {key}={value} {timestamp}"

    replication_threads = []
    for server_ip, server_port in servers:
        if server_ip != leader_ip or server_port != leader_port:
            replication_thread = threading.Thread(target=replicate_to_server, args=(server_ip, server_port, replication_data))
            replication_thread.start()
            replication_threads.append(replication_thread)

    for replication_thread in replication_threads:
        replication_thread.join()


def handle_client(client_socket, address, key_value_store, leader_ip, leader_port):
    while True:
        data = client_socket.recv(1024).decode()
        print(f"Received data: {data} from {address[0]}:{address[1]}")

        if not data:
            break

        processarMensagem(client_socket, address, data, key_value_store, leader_ip, leader_port)
    print(f"Connection closed by {address[0]}:{address[1]}")
    client_socket.close()

def iniciarServidor(ip, port, leader_ip, leader_port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((ip, port))
    server_socket.listen(5)

    print(f"Server listening on {ip}:{port}")
    key_value_store = HashTableKV()

    while True:
        client_socket, address = server_socket.accept()
        print(f"Accepted connection from {address[0]}:{address[1]}")

        # Iniciar uma nova thread para tratar a conexão do cliente
        client_thread = threading.Thread(handle_client(client_socket, address, key_value_store, leader_ip, leader_port))
        client_thread.start()


iniciarServidor(server_ip, server_port, lider_ip, lider_port)
# todo: ervidor para processar corretamente as requisições PUT e retornar a mensagem PUT_OK com o timestamp1, de acordo com o formato esperado pelo cliente.