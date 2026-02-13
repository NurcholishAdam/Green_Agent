class MessageBus:

    def __init__(self):
        self.messages = []

    def publish(self, sender, payload):
        self.messages.append((sender, payload))

    def consume_all(self):
        msgs = self.messages
        self.messages = []
        return msgs
