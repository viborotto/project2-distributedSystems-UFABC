class Mensagem:
    def __init__(self, operacao, message_key, message_value, message_timestamp):
        self.operacao = operacao
        self.message_key = message_key
        self.message_value = message_value
        self.message_timestamp = message_timestamp