import zmq
import threading
import traceback

class Message(object):
    """
    Used for control messages.
    """
    name = 'Message'
    def __init__(self, data=None):
        self.data = data

class ExitMessage(Message):
    name = 'Exit'

class ExceptionMessage(Message):
    name = 'Exception'
    def __init__(self, traceback):
        self.traceback = traceback

class ZmqThread(object):
    context = zmq.Context()
    binds = {}

    @classmethod
    def send(cls, bind, msg):
        send_socket = zmq.Socket(cls.context, zmq.PUSH)
        send_socket.connect(bind)
        if isinstance(msg, Message):
            send_socket.send_json((1, msg.__dict__))
        else:
            send_socket.send_json((0, msg))

    def __init__(self, f, supervisor=None, daemon=True, bind=None, **kwargs):
        """
        f should be a callable function that gets called when this class gets sent a message. 
        If extending this class, overriding the "called" method is acceptable as well.
        **kwargs are any default default arguments sent to the callable.

        The callable should be of form f(message, self.defaults). Message may be 
        any valid serializable python object. Note that the function's return will be ignored.

        Supervisor is a ZmqThread object that will receive errors and statuses.
        """
        #from ipdb import set_trace; set_trace()
        self.f = f
        self.defaults = kwargs
        self.recv_socket = zmq.Socket(self.context, zmq.PULL)
        self.supervisor = supervisor
        self.daemon = daemon
        self.bind = bind or 'inproc://zmqthread-%s'%str(hash(self))
        self.binds[self.bind] = self
        self.recv_socket.bind(self.bind)
        self.start()

    def start(self):
        self.thread = threading.Thread(None, self.main_loop)
        self.thread.daemon = self.daemon
        self.thread.start()

    def __lshift__(self, msg):
        ZmqThread.send(self.bind, msg)


    def main_loop(self):
        if self.supervisor:
            self.supervisor << "Started"
            
        while 1:
            control, msg = self.recv_socket.recv_json()
            if control and msg == 'Exit':
                if self.supervisor:
                    self.supervisor << "Exiting"
                return

            try:
                self.f(msg, self.defaults)
            except Exception, e:
                if self.supervisor:
                    self.supervisor <<  ExceptionMessage(traceback.format_exc())

