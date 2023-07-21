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
def requisitarServidor(server_ip, server_port, mensagem):
    # TCP
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_socket.connect((str(server_ip), int(server_port)))
        serialized_request = pickle.dumps(mensagem)
        client_socket.sendall(serialized_request) # Envia mensagem com a operacao desejada para o S
        resposta_servidor_serializada = client_socket.recv(1024) # recebe todas as operacoes
        mensagem = pickle.loads(resposta_servidor_serializada)
        resposta_operacao = mensagem.operacao
        # TODO: ARMAZENAR A RESPOSTA DO SERVIDOR NA HASH OU ATUALIZAR A KEY
        if len(resposta_servidor_serializada) > 0:
            if resposta_operacao == 'GET_OK':
                print(f"GET key: {mensagem.message_key} value: {mensagem.message_value} obtido do servidor {server_ip}:{server_port}, meu timestamp {timestamp} e do servidor {mensagem.message_timestamp}")
            elif resposta_operacao == 'NULL':
                print(f"GET key: {mensagem.message_key} value: NULL obtido do servidor {server_ip}:{server_port}, meu timestamp {timestamp} e do servidor {mensagem.message_timestamp}")
            elif resposta_operacao == 'TRY_OTHER_SERVER_OR_LATER':
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
    op_kv = input("Defina a operacao e o par key-value(e.g INIT ou GET key ou PUT key=value): ")
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
        if key_value_store_cliente.search(key):
            valueC, timestampC = key_value_store_cliente.get(key)
            mensagem = Mensagem("GET", key, valueC, timestampC)
        elif not key_value_store_cliente.search(key):
            # TODO: precisa inserir no cliente se nao existe na hashtable?
            mensagem = Mensagem("GET", key, None, 0)
        requisitarServidor(server_ips[id_servidor_escolhido], server_ports[id_servidor_escolhido], mensagem)
    elif op_kv.startswith('PUT'):
        # separando operacao de key-value
        key_value = op_kv[4:]
        # Atualizar o timestamp do cliente
        timestamp += 1
        mensagem = Mensagem("PUT", key_value.split('=')[0], key_value.split('=')[1], timestamp)
        # escolhe aleatoriamente um dos 3 servidores
        id_servidor_escolhido = random.randint(0, 2)
        # id_servidor_escolhido = 0
        requisitarServidor(server_ips[id_servidor_escolhido], server_ports[id_servidor_escolhido], mensagem)
        