import socket, os, threading, random, pickle
from mensagem import Mensagem

# Capturar IPs e portas dos servidores do teclado
server_ips = []
server_ports = []


class HashTableCliente:
    def __init__(self):
        self.store = dict()
        self.timestamps = {}

    def getStore(self):
        return self.store

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
        valueCliente, timestampCliente  = key_value_store_cliente.get(key_servidor)
        if len(resposta_servidor_serializada) > 0:
            # Condicoes de resposta do
            if resposta_operacao == 'GET_OK':
                # caso ja exista na tabela atualizar value e ts
                key_value_store_cliente.put("CHAVE", "VALOR", 3)
                if key_servidor not in key_value_store_cliente.store:
                    print("NAO Tem a chave na TABELA")
                    key_value_store_cliente.put(mensagem.message_key, value_servidor, 0)
                    timestamp_cliente = key_value_store_cliente.getTimestampCliente(key_servidor)
                    print(
                        f"GET key: {key_servidor} value: {value_servidor} obtido do servidor {server_ip}:{server_port}, "
                        f"meu timestamp {timestamp_cliente} e do servidor {timestamp_servidor}")
                    timestamp_cliente = timestamp_servidor
                    key_value_store_cliente.update(key_servidor, value_servidor, timestamp_cliente)

                elif key_servidor in key_value_store_cliente.store:
                    print("Tem a chave na TABELA")
                    timestamp_cliente = key_value_store_cliente.getTimestampCliente(key_servidor)
                    print(
                        f"GET key: {key_servidor} value: {value_servidor} obtido do servidor {server_ip}:{server_port}, "
                        f"meu timestamp {timestamp_cliente} e do servidor {timestamp_servidor}")
                    timestamp_cliente = timestamp_servidor
                    key_value_store_cliente.update(key_servidor, value_servidor, timestamp_cliente)
            elif resposta_operacao == 'NULL':
                # Definido que timestamp 0, nao existe
                print(f"GET key: {mensagem.message_key} value: NULL obtido do servidor {server_ip}:{server_port}, meu timestamp {0} e do servidor {timestamp_servidor}")
            elif resposta_operacao == 'TRY_OTHER_SERVER_OR_LATER':
                ## Aqui a chave ja seria conhecida pelo cliente mas nao tem no servidor um valor atualizado
                # portanto colocaria no cliente com o valor ja conhecido
                # caso ja exista na tabela atualizar value e ts
                if key_value_store_cliente.search(key_servidor):
                    key_value_store_cliente.update(key_servidor, value_servidor, timestamp_servidor)
                elif not key_value_store_cliente.search(key_servidor):
                    key_value_store_cliente.put(key_servidor, value_servidor, timestamp_servidor)
                print('TRY_OTHER_SERVER_OR_LATER')
            elif resposta_operacao == 'PUT_OK':
                print(f"PUT_OK key: {mensagem.message_key} value {mensagem.message_value} timestamp {timestamp_servidor} realizada no servidor {server_ip}:{server_port}")
        else:
            print("Empty response received from server")
    except Exception as e:
        print("Error communicating with server:", str(e))


    finally:
        client_socket.close()


lider_ip = ''
lider_port = ''
# Inicializando estrutura para armazenar informacoes no Cliente
key_value_store_cliente = HashTableCliente()
# Executar requisições GET e PUT
while True:
    op_kv = input("Defina a operacao e o par key-value(e.g INIT | GET key | PUT key=value | TRY GET key | EXIT): ")
    if op_kv == 'INIT':
        defineServidores()
        lider_ip = server_ips[0]
        lider_port = server_ports[0]
    if op_kv.lower() == 'exit':
        break
    ## SIMULAR TRY_ANOTHER_SERVER, TimestampCliente > TimestampServidor
    elif op_kv.startswith('TRY'):
        # Enviar a mensagem com o timestamp maior que o do Servidor
        operacao, _, key = op_kv.partition(' ')
        # escolhe aleatoriamente um dos 3 servidores
        id_servidor_escolhido = random.randint(0, 2)
        # forcando um timestamp maior para simular
        mensagem = Mensagem("TRY", key, "", 1000)
        requisitarServidor(server_ips[id_servidor_escolhido], server_ports[id_servidor_escolhido], mensagem,
                           key_value_store_cliente)
    elif op_kv.startswith('GET'):
        key = op_kv[4:]  # Extrair a parte do comando após "GET " ou seja a key
        # escolhe aleatoriamente um dos 3 servidores
        id_servidor_escolhido = random.randint(0, 2)
        mensagem = Mensagem("GET", key, "", 0)
        requisitarServidor(server_ips[id_servidor_escolhido], server_ports[id_servidor_escolhido], mensagem, key_value_store_cliente)
    elif op_kv.startswith('PUT'):
        timestamp = 0
        # separando operacao de key-value
        key_value = op_kv[4:]
        key = key_value.split('=')[0]
        value = key_value.split('=')[1]
        # Atualizar o timestamp do cliente
        if key in key_value_store_cliente.store:
            timestamp = key_value_store_cliente.getTimestampCliente(key) + 1
            key_value_store_cliente.update(key, value, timestamp)
        else:
            key_value_store_cliente.put(key, value, timestamp)
        mensagem = Mensagem("PUT", key, value, timestamp)
        # escolhe aleatoriamente um dos 3 servidores
        id_servidor_escolhido = random.randint(0, 2)
        requisitarServidor(server_ips[id_servidor_escolhido], server_ports[id_servidor_escolhido], mensagem,
                           key_value_store_cliente)
