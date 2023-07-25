import socket, os, threading, random, pickle
from mensagem import Mensagem

# Capturar IPs e portas dos servidores do teclado
server_ips = []
server_ports = []
# Inicializar o timestamp do cliente
timestamp = 0

class HashTableCliente:
    def __init__(self):
        self.store = {}
        self.timestamps = {}

    def put(self, key_cliente, value_cliente, timestamp_cliente):
        # Verifica se a chave já existe na tabela
        if self.search(key_cliente):
            # Atualiza o valor e o timestamp associados à chave
            self.store[key_cliente] = value_cliente
            self.timestamps[key_cliente] = timestamp_cliente
        else:
            # Insere a nova chave na tabela
            self.store[key_cliente] = value_cliente
            self.timestamps[key_cliente] = timestamp_cliente

    def get(self, key_cliente):
        value_cliente = self.store.get(key_cliente)
        timestamp_cliente = self.timestamps.get(key_cliente)
        return value_cliente, timestamp_cliente

    def search(self, key):
        return key in self.store

    def update(self, key_cliente, new_value, new_timestamp):
        if key in self.store:
            self.store[key_cliente] = new_value
            self.timestamps[key_cliente] = new_timestamp
        else:
            raise KeyError("Key does not exist in KeyValueStore")

def defineServidores():
    # for i in range(3):
        # server_ip = input("Defina o IP do Servidor {i+1}:(default 127.0.0.1) ") or "127.0.0.1"
        # server_port = input("Defina a porta do Servidor {i+1}:(10097, 10098, 10099) ")
        server1_ip = '127.0.0.1'
        server1_port = 10097
        server2_ip = '127.0.0.1'
        server2_port = 10098
        server3_ip = '127.0.0.1'
        server3_port = 10099
        server_ips.append(server1_ip)
        server_ports.append(int(server1_port))
        server_ips.append(server2_ip)
        server_ports.append(int(server2_port))
        server_ips.append(server3_ip)
        server_ports.append(int(server3_port))
        # server_ips.append(server_ip)
        # server_ports.append(int(server_port))


# estabelecer a conexao com um servidor, enviar a requisicao e receber a resposta
def requisitarServidor(server_ip, server_port, mensagem, key_value_store_cliente):
    # TCP
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_socket.connect((str(server_ip), int(server_port)))
        serialized_request = pickle.dumps(mensagem)
        client_socket.sendall(serialized_request) # Envia mensagem com a operacao desejada para o S
        resposta_servidor_serializada = client_socket.recv(1024) # recebe todas as operacoes
        mensagem_servidor = pickle.loads(resposta_servidor_serializada)
        resposta_operacao = mensagem_servidor.operacao
        key_servidor = mensagem_servidor.message_key
        value_servidor = mensagem_servidor.message_value
        timestamp_servidor = mensagem_servidor.message_timestamp
        valueCliente, timestampCliente  = key_value_store_cliente.get(key)
        if len(resposta_servidor_serializada) > 0:
            if resposta_operacao == 'GET_OK':
                #caso ja exista na tabela atualizar value e ts
                # INSERIR NA TABELA COM VALOR INICIALIZADO QUANDO MANDA O GET PARA O SERVIDOR E AQUI SO ATUALIZAR
                print(f"GET key: {mensagem.message_key} value: {value_servidor} obtido do servidor {server_ip}:{server_port}, meu timestamp {timestampCliente} e do servidor {timestamp_servidor}")
                key_value_store_cliente.update(key_servidor, value_servidor, timestamp)
            elif resposta_operacao == 'NULL':
                # Definido que timestamp 0, nao existe
                print(f"GET key: {mensagem.message_key} value: NULL obtido do servidor {server_ip}:{server_port}, meu timestamp {timestamp} e do servidor {timestamp_servidor}")
            elif resposta_operacao == 'TRY_OTHER_SERVER_OR_LATER':
                ## Aqui a chave ja seria conhecida pelo cliente mas nao tem no servidor um valor atualizado
                # portanto colocaria no cliente com o valor ja conhecido
                # caso ja exista na tabela atualizar value e ts
                if key_value_store_cliente.search(key_servidor):
                    key_value_store_cliente.update(key_servidor, value_servidor, timestamp)
                elif not key_value_store_cliente.search(key_servidor):
                    key_value_store_cliente.put(key_servidor, value_servidor, timestamp)
                print('TRY_OTHER_SERVER_OR_LATER')
            elif resposta_operacao == 'PUT_OK':
                print(f"PUT_OK key: {mensagem.message_key} value {mensagem.message_value} timestamp {mensagem.message_timestamp} realizada no servidor {server_ip}:{server_port}")
        else:
            print("Empty response received from server")
    except Exception as e:
        print("Error communicating with server:", str(e))


    finally:
        client_socket.close()


lider_ip = ''
lider_port = ''
# Executar requisições GET e PUT
while True:
    key_value_store_cliente = HashTableCliente()
    op_kv = input("Defina a operacao e o par key-value(e.g INIT | GET key | PUT key=value | EXIT): ")
    if op_kv == 'INIT':
        defineServidores()
        lider_ip = server_ips[0]
        lider_port = server_ports[0]
    if op_kv.lower() == 'exit':
        break
    elif op_kv.startswith('GET'):
        key = op_kv[4:]  # Extrair a parte do comando após "GET "
        # escolhe aleatoriamente um dos 3 servidores
        id_servidor_escolhido = random.randint(0, 2)
        # TODO: # Substitua 'timestamp1' pelo valor correto do último timestamp conhecido
            # envie a key e o último timestamp que o cliente tem associado a essa key
            # Se existe na tabela pegar o value e ts e enviar para o servidor
        #TODO: NAO CAI NESSA PRIMEIRA CONDICAO
        if key_value_store_cliente.search(key):
            print("Tem a chave na HTC antes de enviar para o servidor o GET")
            valueC, timestampC = key_value_store_cliente.get(key)
            mensagem = Mensagem("GET", key, valueC, timestampC)
        else:
            print("NAO Tem a chave na HTC antes de enviar para o servidor o GET")
            key_value_store_cliente.put(key, None, 0)
            print(key_value_store_cliente.get(key))
            mensagem = Mensagem("GET", key, None, 0)
        requisitarServidor(server_ips[id_servidor_escolhido], server_ports[id_servidor_escolhido], mensagem, key_value_store_cliente)
    elif op_kv.startswith('PUT'):
        # separando operacao de key-value
        key_value = op_kv[4:]
        # Atualizar o timestamp do cliente
        timestamp += 1
        mensagem = Mensagem("PUT", key_value.split('=')[0], key_value.split('=')[1], timestamp)
        # escolhe aleatoriamente um dos 3 servidores
        id_servidor_escolhido = random.randint(0, 2)
        requisitarServidor(server_ips[id_servidor_escolhido], server_ports[id_servidor_escolhido], mensagem, key_value_store_cliente)
        