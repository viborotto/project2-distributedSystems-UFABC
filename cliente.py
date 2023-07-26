import socket, random, pickle
from mensagem import Mensagem

class HashTableCliente:
    def __init__(self):
        self.store = dict()
        self.timestamps = {}

    def put(self, key_cliente, value_cliente, timestamp_cliente):
        self.store[key_cliente] = value_cliente
        self.timestamps[key_cliente] = timestamp_cliente

    def get(self, key_cliente):
        value_cliente = self.store.get(key_cliente)
        timestamp_cliente = self.timestamps.get(key_cliente)
        return value_cliente, timestamp_cliente

    def getTimestampCliente(self, key_cliente):
        timestamp_cliente = self.timestamps.get(key_cliente)
        return timestamp_cliente

    def search(self, key_cliente):
        return key_cliente in self.store

    def update(self, key_cliente, new_value, new_timestamp):
        if key_cliente in self.store:  # Corrigir o acesso à chave aqui
            self.store[key_cliente] = new_value
            self.timestamps[key_cliente] = new_timestamp
        else:
            raise KeyError("Key does not exist in KeyValueStore")

# Define o ip e porta dos 3 servidores, e coloca em uma lista
def defineServidores():
    for i in range(3):
        server_ip = input("Defina o IP do Servidor " + str(i+1) + " (default 127.0.0.1): ") or "127.0.0.1"
        server_port = input("Defina a porta do Servidor " + str(i+1) + " (10097, 10098, 10099): ")
        server_ips.append(server_ip)
        server_ports.append(int(server_port))
    print("\n")

# estabelecer a conexao com um servidor
def conectarServidor(server_ip, server_port):
    # TCP
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_socket.connect((str(server_ip), int(server_port)))
        return client_socket
    except Exception as e:
        print("Erro conectando com servidor:", str(e))
        return None

# enviar a Mensagem serializada para o Servidor
def enviarMensagem(client_socket, mensagem):
    try:
        serialized_request = pickle.dumps(mensagem)
        # Envia mensagem com a operacao desejada para o S
        client_socket.sendall(serialized_request)
    except Exception as e:
        print("Erro ao enviar a mensagem:", str(e))

# receber resposta do Servidor e desserializar
def receberResposta(client_socket):
    try:
        resposta_servidor_serializada = client_socket.recv(1024)
        return pickle.loads(resposta_servidor_serializada)
    except Exception as e:
        print("Erro ao receber resposta do Servidor:", str(e))
        return None

# fechar conexao do socket
def fecharConexao(client_socket):
    try:
        client_socket.close()
    except Exception as e:
        print("Erro ao fechar conexao:", str(e))

# processar resposta do servidor para os cenarios de GET_OK, NULL, TRY_OTHER_SERVER_OR_LATER, PUT_OK
def processarResposta(resposta_servidor, server_ip, server_port, key_value_store_cliente):
    resposta_operacao = resposta_servidor.operacao
    key_servidor = resposta_servidor.message_key
    value_servidor = resposta_servidor.message_value
    timestamp_servidor = resposta_servidor.message_timestamp

    if resposta_operacao == 'GET_OK':
        # caso já exista na tabela atualizar value e ts
        if key_servidor not in key_value_store_cliente.store:
            key_value_store_cliente.put(resposta_servidor.message_key, value_servidor, 0)
            timestamp_cliente = key_value_store_cliente.getTimestampCliente(key_servidor)
            print(
                f"GET key: {key_servidor} value: {value_servidor} obtido do servidor {server_ip}:{server_port}, "
                f"meu timestamp {timestamp_cliente} e do servidor {timestamp_servidor}")
            timestamp_cliente = timestamp_servidor
            key_value_store_cliente.update(key_servidor, value_servidor, timestamp_cliente)
        elif key_servidor in key_value_store_cliente.store:
            timestamp_cliente = key_value_store_cliente.getTimestampCliente(key_servidor)
            print(
                f"GET key: {key_servidor} value: {value_servidor} obtido do servidor {server_ip}:{server_port}, "
                f"meu timestamp {timestamp_cliente} e do servidor {timestamp_servidor}")
            timestamp_cliente = timestamp_servidor
            key_value_store_cliente.update(key_servidor, value_servidor, timestamp_cliente)
    elif resposta_operacao == 'NULL':
        # Definido que timestamp 0, não existe
        print(f"NULL: GET key: {resposta_servidor.message_key}")
    elif resposta_operacao == 'TRY_OTHER_SERVER_OR_LATER':
        # Aqui a chave já seria conhecida pelo cliente, mas não tem no servidor um valor atualizado
        # portanto colocaria no cliente com o valor já conhecido
        # caso já exista na tabela atualizar value e ts
        if key_value_store_cliente.search(key_servidor):
            key_value_store_cliente.update(key_servidor, value_servidor, timestamp_servidor)
        elif not key_value_store_cliente.search(key_servidor):
            key_value_store_cliente.put(key_servidor, value_servidor, timestamp_servidor)
        print('TRY_OTHER_SERVER_OR_LATER')
    elif resposta_operacao == 'PUT_OK':
        print(
            f"PUT_OK key: {resposta_servidor.message_key} value {resposta_servidor.message_value} "
            f"timestamp {timestamp_servidor} realizada no servidor {server_ip}:{server_port}")

def requisitarServidor(server_ip, server_port, mensagem, key_value_store_cliente):
    client_socket = conectarServidor(server_ip, server_port)

    if client_socket is not None:
        enviarMensagem(client_socket, mensagem)
        resposta_servidor = receberResposta(client_socket)
        if resposta_servidor is not None:
            processarResposta(resposta_servidor, server_ip, server_port, key_value_store_cliente)
        fecharConexao(client_socket)


# Capturar IPs e portas dos servidores do teclado
server_ips = []
server_ports = []
# Inicializando estrutura para armazenar informacoes no Cliente
key_value_store_cliente = HashTableCliente()

# Executar operacoes
while True:
    op_kv = input("Defina a operacao e o par key-value(e.g INIT | GET key | PUT key=value | GET TRY | EXIT): ")
    if op_kv == 'INIT':
        defineServidores()
    if op_kv.lower() == 'exit':
        break

    elif op_kv.startswith('GET'):
        key = op_kv[4:]  # Extrair a parte do comando após "GET " ou seja a key
        timestamp_get = 0
        ## SIMULAR TRY_ANOTHER_SERVER, TimestampCliente > TimestampServidor
        if key == "TRY":
            # para simular o TRY_ANOTHER_SERVER
            timestamp_get = 1000
            key_value_store_cliente.put(key, "", timestamp_get)
        # escolhe aleatoriamente um dos 3 servidores
        id_servidor_escolhido = random.randint(0, 2)
        mensagem = Mensagem("GET", key, "", timestamp_get)
        requisitarServidor(server_ips[id_servidor_escolhido], server_ports[id_servidor_escolhido], mensagem, key_value_store_cliente)
    elif op_kv.startswith('PUT'):
        timestamp = 0
        # separando operacao de key-value
        key_value = op_kv[4:]
        key = key_value.split('=')[0]
        value = key_value.split('=')[1]
        # caso o put seja de uma chave que ja existe pegamos as informacoes mais recentes no cliente
        ## e enviamos para o servidor
        if key_value_store_cliente.search(key):
            # timestamp = key_value_store_cliente.getTimestampCliente(key) + 1
            timestamp = key_value_store_cliente.getTimestampCliente(key)
            key_value_store_cliente.update(key, value, timestamp)
        # se nao tiver ainda a chave inicializa ela local no cliente
        else:
            key_value_store_cliente.put(key, value, timestamp)
        mensagem = Mensagem("PUT", key, value, timestamp)
        # escolhe aleatoriamente um dos 3 servidores
        id_servidor_escolhido = random.randint(0, 2)
        requisitarServidor(server_ips[id_servidor_escolhido], server_ports[id_servidor_escolhido], mensagem,
                           key_value_store_cliente)
