import socket, os, threading, random, pickle

# Capturar IPs e portas dos servidores do teclado
server_ips = []
server_ports = []
# Inicializar o timestamp do cliente
timestamp = 0

class Mensagem:
    def __init__(self, operacao, message_key, message_value, message_timestamp):
        self.operacao = operacao
        self.message_key = message_key
        self.message_value = message_value
        self.message_timestamp = message_timestamp

def defineServidores():
    # for i in range(3):
        # server_ip = input("Defina o IP do Servidor {i+1}:(default 127.0.0.1) ")
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
        print("mensagem: ", mensagem)
        serialized_request = pickle.dumps(mensagem)
        client_socket.sendall(serialized_request)
        resposta_servidor_serializada = client_socket.recv(1024)
        if len(resposta_servidor_serializada) > 0:
            resposta = pickle.loads(resposta_servidor_serializada)
            print("Response:", resposta)
        else:
            print("Empty response received from server")
    except Exception as e:
        print("Error communicating with server:", str(e))


    finally:
        client_socket.close()



# eleger o lider aleatorio? usar uma flag de lider?
lider_ip = ''
lider_port = ''
# Executar requisições GET e PUT
while True:
    op_kv = input("Defina a operacao e o par key-value(e.g INIT ou GET key ou PUT key=value): ")
    # op_kv = input("Defina a operacao INIT, PUT, ou GET")
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
        #TODO: # Substitua 'timestamp1' pelo valor correto do último timestamp conhecido
        mensagem = Mensagem("GET", key, None, timestamp)
        requisitarServidor(server_ips[id_servidor_escolhido], server_ports[id_servidor_escolhido], op_kv)
    elif op_kv.startswith('PUT'):
        # separando operacao de key-value
        key_value = op_kv[4:]
        # Atualizar o timestamp do cliente
        timestamp += 1
        mensagem = Mensagem("PUT", key_value.split('=')[0], key_value.split('=')[1], timestamp)
        # escolhe aleatoriamente um dos 3 servidores
        # id_servidor_escolhido = random.randint(0, 2)
        id_servidor_escolhido = 1
        print("porta servidor escolhido: ", server_ports[id_servidor_escolhido])
        print(type(server_ports[id_servidor_escolhido]))
        print("ip servidor escolhido: ", server_ips[id_servidor_escolhido])
        print(type(server_ips[id_servidor_escolhido]))
        # TODO: Na requisição do PUT, envie a key e a value.
        requisitarServidor(server_ips[id_servidor_escolhido], server_ports[id_servidor_escolhido], mensagem)







