import zmq
import threading
import traceback
import time
import uuid
import copy

def get_random():
    return str(uuid.uuid4()).replace('-','')

class BaseMessage(object):
    name = "BaseMessage"
    def __init__(self, message=None, sender=None, data=None, receiver=None, message_id=None):
        message = message if message is not None else {}

        self.sender = message.get('sender', sender)
        self.data = message.get('data', data)

        if not self.sender:
            raise Exception('Invalid sender')

        self.receiver = receiver
        self.message_id = message_id or get_random()

    def reply(self, data):
        if not self.receiver:
            raise Exception('This message was not received')
        self.receiver.send(self.sender, data)

    def get(self, key, *args, **kwargs):
        return getattr(self, key, *args, **kwargs)

class Message(BaseMessage):
    name = 'Message'


class ExitMessage(BaseMessage):
    name = 'Exit'
    def __init__(self, sender=None, data=None, receiver=None):
        super(ExitMessage, self).__init__(sender=sender, data="Exit", receiver=receiver)

class ExceptionMessage(BaseMessage):
    name = 'Exception'

class Node(object):
    _context = None
    _message_types = {'Message':Message, 'Exit':ExitMessage, 'Exception':ExceptionMessage }
    def __init__(self, f, bind=None, daemon=True, **kwargs):
        #f is a callable that will get sent message info. Should be passed unless this is a base class.
        Node._context = Node._context or zmq.Context()

        self.f = f
        self.id = bind or 'inproc://homingbird-' + str(hash(self))

        if kwargs.get('connect_socket', True):
            self.connect_socket()
        
        t = threading.Thread(None, self.main)
        t.daemon = daemon
        t.start()

    def connect_socket(self):
        self.recv_socket = self._context.socket(zmq.PULL)
        self.recv_socket.bind(self.id)

    def send_message(self, to, message):
        if isinstance(to, Node):
            to = to.id
        socket = self._context.socket(zmq.PUSH)
        socket.connect(to)
        socket.send_pyobj(message)

    def send(self, to, data):
        m = Message(sender=self.id, data=data)
        self.send_message(to, m)
        
    def get_message(self):
        m= self.recv_socket.recv_pyobj()
        return m

    def decompose(self, message):
        if not message or not self._message_types.get(message.get('name', None)):
            raise Exception('Invalid message type or parameters.')

        message_type = self._message_types[message.get('name')]
        m = message_type(message, receiver=self)
        return m

    def report(self, m):
        #if m.name == 'Exit':
        #    print '%s exiting'%self.bind
        print m.__dict__

    def exit(self):
        m = ExitMessage(sender=self.id)
        self.send_message(self, m)

    def receive(self, timeout=None):
        if timeout:
            p = zmq.Poller()
            p.register(self.recv_socket)

            start = time.time()
            while 1:
                r = p.poll(100) #poll for 100 ms, see if we're past our threshold
                if r: break
                if time.time() - start > timeout:
                    return None
        m = self.decompose(self.get_message())
        return m

    def main(self):
        while 1:
            m = self.receive(1)
            if not m: continue 
            if m.name == 'Exit':
                self.report(m)
                break

            elif m.name == 'Message':
                self.f(m)

class LocalNode(Node):
    """Local Nodes allow you to connect to remote nodes with a different bind, or to get synchronous responses from Nodes."""
    def __init__(self, bind=None, daemon=True):
        Node._context = Node._context or zmq.Context()
        self.id = bind or 'inproc://homingbird-' + str(hash(self))
        self.connect_socket()

    def main(self):
        return

if __name__ == '__main__':
    def ping(m):
        print "%s received"%m.data
        m.reply("Ping")

    def pong(m):
        print "%s received."%m.data
        m.reply("Pong")


    c = zmq.Context()
    n1 = Node(ping)
    n2 = Node(pong)

    n1.send(n2, "Ping")
    time.sleep(4)

