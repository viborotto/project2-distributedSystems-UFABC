import socket, os, random, pickle, threading
from mensagem import Mensagem

# Capturar IP e porta do servidor
server_ip = input("Defina o IP do Servidor (default: 127.0.0.1): ") or "127.0.0.1"
server_port = int(input("Defina a PORTA do Servidor(10097, 10098 e 10099): ") or "10097")
print('server_ip: ', server_ip)
print('server_port: ', server_port)

# Capturar IP e porta do líder
lider_ip = input("Defina o IP do Servidor LIDER (default: 127.0.0.1): ") or "127.0.0.1"
lider_port = int(input("Defina a PORTA do Servidor LIDER (10097, 10098 e 10099): ") or "10098")
print('leader_ip: ', lider_ip)
print('lider_port: ', lider_port)

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
def processarMensagem(client_socket, address, mensagem, key_value_store, leader_ip, leader_port, client_ip, client_port):
    # Implemente aqui a lógica para processar a requisição recebida do cliente
    # Se o servidor não for o líder, encaminhe a requisição para o líder
    # Caso contrário, se o comando for PUT, insira a chave-valor na tabela de hash local com o timestamp atual
    # Se o comando for GET, obtenha o valor e o timestamp associados à chave

    if mensagem.operacao == "REPLICATION":
        key = mensagem.message_key
        value = mensagem.message_value
        # pega o timestamp da mensagem recebida do lider e atualizar ou insere
        timestamp = mensagem.message_timestamp
        replication_from_lider = f"REPLICATION key:{key} value:{value} ts:{timestamp}."
        print(replication_from_lider)
        # O VERIFICAR SE A KEY JA EXISTE NO HASHTABLE
        if key_value_store.search(key):
            # SE EXISTIR: ATUALIZA O VALOR E TIMESTAMP
            print("chave ja existe")
            key_value_store.update(key, value, timestamp)
            print("atualizado o valor e timestamp da chave: " + value)
            print(key_value_store.get(key))
            # response = f"REPLICATION_OK"
            mensagem_put = Mensagem("REPLICATION_OK", key, value, timestamp)
            response = pickle.dumps(mensagem_put)
            print(response)
        else:
            # INSERE KEY E VALUE EM UMA TABELA DE HASHLOCAL
            key_value_store.put(key, value, timestamp)
            print("Chave foi colocada no Hash? ", key_value_store.search(key))
            print(key_value_store.get(key))
            # Somente depois do replication enviar REPLICATION_OK para o lider
            # response = f"REPLICATION_OK"
            mensagem_put = Mensagem("REPLICATION_OK", key, value, timestamp)
            response = pickle.dumps(mensagem_put)
            print(response)

    # Caso seja Put e NAO SEJA LIDER
    if mensagem.operacao == 'PUT' and (server_ip != lider_ip or lider_port != server_port):
        # REDIRECIONA A REQUISICAO PARA O LIDER
        print("NAO SOU O SERVIDOR LIDER")
        print("REDIRECIONANDO REQUEST PARA O LIDER")
        # TODO: AJUSTAR ESSE CENARIO
        print(f"Encaminhando PUT key:{mensagem.message_key} value:{mensagem.message_value}")
        response = forward_request_to_leader(leader_ip, leader_port, mensagem).decode()
        print("FORWARD PRINT RESPONSE", response)
    else:
        if mensagem.operacao == 'PUT':
            key = mensagem.message_key
            value = mensagem.message_value
            timestamp = key_value_store.timestamps.get(key, 0) + 1
            # TODO: COMO PEGAR AS INFORMACOES DO CLIENTE PARA O PRINT?
            # print(f"Cliente {IP}:{porta} PUT key:{mensagem.message_key} value:{mensagem.message_value}")
            # O VERIFICAR SE A KEY JA EXISTE NO HASHTABLE
            if key_value_store.search(key):
                # SE EXISTIR: ATUALIZA O VALOR E TIMESTAMP
                print("chave ja existe")
                key_value_store.update(key, value, timestamp)
                print("atualizado o valor e timestamp da chave: " + value)
                print(key_value_store.get(key))
                replicate_to_servers(key, value, timestamp, leader_ip, leader_port)
                # response = f"PUT_OK {timestamp}"
                mensagem_put = Mensagem("PUT_OK", key, value, timestamp)
                response = pickle.dumps(mensagem_put)
            else:
                # INSERE KEY E VALUE EM UMA TABELA DE HASHLOCAL
                key_value_store.put(key, value, timestamp)
                print("Chave foi colocada no Hash? ", key_value_store.search(key))
                print(key_value_store.get(key))
                replicate_to_servers(key, value, timestamp, leader_ip, leader_port)
                # Mensagem: REPLICATION key value timestamp
                # Somente depois do replication enviar PUT_OK para o cliente
                # response = f"PUT_OK {timestamp}"
                mensagem_put = Mensagem("PUT_OK", key, value, timestamp)
                response = pickle.dumps(mensagem_put)
                print(response)
        elif mensagem.operacao == 'GET':
            key = mensagem.message_key
            value = mensagem.message_value
            valueS, timestampS = key_value_store.get(key)
            timestampCliente = mensagem.message_timestamp
            # 1. Caso nao exista
            if not key_value_store.search(key):
                # # todo: o que enviar para o cliente? mensagem = Mensagem("NULL", key, value, timestamp)
                mensagem_get = Mensagem("NULL", key, 'NULL', 0)
                response = pickle.dumps(mensagem_get)
                print(f"Cliente {client_ip}:{client_port} GET key:{key} ts:{timestampCliente}. Meu ts é {timestampS}, portanto devolvendo {mensagem_get.operacao}")
            # 2. Caso exista
            else:
                # devolver o value que possui o timestampS o qual timestampS >= timestampX
                # exemplo se receber um Tx = 2 e tiver o Ts=3 => value = valueS timestampS
                if timestampS >= timestampCliente:
                    mensagem.message_value = valueS
                # todo: o que enviar para o cliente? mensagem = Mensagem("GET_OK", key, valueS, timestampS)
                #     response = f"GET_OK"
                    mensagem_get = Mensagem("GET_OK", key, valueS, timestampS)
                    response = pickle.dumps(mensagem_get)
                    print(
                        f"Cliente {client_ip}:{client_port} GET key:{key} ts:{timestampCliente}. Meu ts é {timestampS}, portanto devolvendo {valueS}")
                # Nesse caso significa que a chave em S estaria desatualizada:
                elif timestampS < timestampCliente:
                    # todo: o que enviar para o cliente? mensagem = Mensagem("TRY_OTHER_SERVER_OR_LATER", key, value, timestamp)
                    # response = f"TRY_OTHER_SERVER_OR_LATER"
                    mensagem_get = Mensagem("TRY_OTHER_SERVER_OR_LATER", key, value, timestampCliente)
                    response = pickle.dumps(mensagem_get)
                    print(
                        f"Cliente {client_ip}:{client_port} GET key:{key} ts:{timestampCliente}. Meu ts é {timestampS}, portanto devolvendo {value}")

    # PRA CADA OPERACAO ENVIA PARA O CLIENTE O RESPONSE
    client_socket.sendall(response)
    # client_socket.sendall(response.encode())

def forward_request_to_leader(leader_ip, leader_port, mensagem):
    # cria novo socket pra conexao com o lider
    leader_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        leader_socket.connect((leader_ip, leader_port))

        serialized_request = pickle.dumps(mensagem)
        leader_socket.sendall(serialized_request)  # envia mensagem PUT para o lider
        response_forward_request_to_leader = leader_socket.recv(1024)
        print(response_forward_request_to_leader.decode())
        return response_forward_request_to_leader
    finally:
        leader_socket.close()

def replicate_to_server(server_ip_rep, server_port_rep, mensagem):
    replication_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        replication_socket.connect((server_ip_rep, server_port_rep))
        # VERIFICAR O SEND E O RECV
        serialized_request = pickle.dumps(mensagem)
        replication_socket.sendall(serialized_request) # envia mensagem com REPLICATION
        response_rep = replication_socket.recv(1024) # recebe Mensagens dos servidores
        mensagem = pickle.loads(response_rep)
        resposta_operacao = mensagem.operacao # OPERACAO REPLICATION_OK
        print(resposta_operacao)
        print(f"Replication response from {server_ip_rep}:{server_port_rep}")
        # if len(response_rep) > 0:
        #     resposta = pickle.loads(response_rep)
        #     print("Response:", resposta)
        # else:
        #     print("Empty response received from server")
    finally:
        replication_socket.close()

def replicate_to_servers(key, value, timestamp, leader_ip, leader_port):
    servers = [
        ("127.0.0.1", 10097),  # Endereço do servidor 1
        ("127.0.0.1", 10098),  # Endereço do servidor 2
        ("127.0.0.1", 10099)  # Endereço do servidor 3
    ]
    print(type(leader_ip))
    print(type(leader_port))
    servers.remove((leader_ip, leader_port))

    replication_data = f"REPLICATION {key}={value} {timestamp}"
    mensagem = Mensagem("REPLICATION", key, value, timestamp)

    print(replication_data)

    replication_threads = []
    for server_ip_rep, server_port_rep in servers:
        if server_ip_rep != leader_ip or server_port_rep != leader_port:
            replication_thread = threading.Thread(target=replicate_to_server,
                                                  args=(server_ip_rep, server_port_rep, mensagem))
            replication_thread.start()
            replication_threads.append(replication_thread)
    # TODO: quando o lider receber REPLICATION_OK printar:
    # put_ok_cliente = f"Enviando PUT_OK ao Cliente {IP}:{porta} da key:{key} ts:[timestamp_do_servidor]"

    for replication_thread in replication_threads:
        replication_thread.join()


def handle_client(client_socket, address, key_value_store, leader_ip, leader_port, client_ip, client_port):
    while True:
        data = client_socket.recv(1024)

        if not data:
            break

        mensagem = pickle.loads(data)
        print(f"Received data: {mensagem.message_key} = {mensagem.message_value} from {address[0]}:{address[1]}")
        processarMensagem(client_socket, address, mensagem, key_value_store, leader_ip, leader_port, client_ip, client_port)
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
        client_ip = address[0]
        client_port = address[1]
        # Iniciar uma nova thread para tratar a conexão do cliente
        client_thread = threading.Thread(target=handle_client,
                                         args=(client_socket, address, key_value_store, lider_ip, lider_port, client_ip, client_port))
        client_thread.start()


iniciarServidor(server_ip, server_port, lider_ip, lider_port)
# todo: ervidor para processar corretamente as requisições PUT e retornar a mensagem PUT_OK com o timestamp1, de acordo com o formato esperado pelo cliente.