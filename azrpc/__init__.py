import os
import time
import gevent
import logging
import cPickle
import zmq.green as zmq

from uuid import uuid4
from types import GeneratorType
from gevent import GreenletExit
from gevent.queue import Queue
from gevent.event import AsyncResult

from functionregister import FunctionRegister

log = logging.getLogger(__name__)


class AZRPCTimeout(Exception):
    pass

class AZRPCStop(Exception):
    pass


# Client messages
CLI_SPAWN = '\x00'
CLI_SPAWN_SAFE = '\x01'
CLI_RUN = '\x02'
CLI_EXECUTE = '\x03'
CLI_PONG = '\x04'
CLI_STREAM = '\x05'
CLI_STREAM_SYNC = '\x06'
CLI_ACKNOWLEDGED = '\x07'
CLI_CANCEL = '\x08'

# Server messages
SRV_OK = '\xa0'
SRV_STARTED = '\xa1'
SRV_ERROR = '\xa2'
SRV_HEARTBEAT = '\xa3'
SRV_PING = '\xa4'
SRV_STREAM = '\xa5'
SRV_STREAM_SYNC = '\xa6'
SRV_CANCEL = '\xa7'

# Data types
DAT_RAW = '\xf0'
DAT_PICKLE = '\xf1'

ctx = zmq.Context.instance()

ipc_prefix = '/tmp/azrpc-'


class AZRPC(FunctionRegister):
    """Base RPC class"""

    identity = None
    ipc = None
    control_greenlet = None

    def __init__(self, identity, port=5571, heartbeat_interval=1, heartbeat_timeout=3, client_timeout=30):
        assert heartbeat_timeout > heartbeat_interval
        assert client_timeout > heartbeat_timeout
        super(AZRPC, self).__init__()
        if identity:
            self._set_identity(identity)
        self.port = port
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.client_timeout = client_timeout
        self.clients = dict()

    def _set_identity(self, identity):
        """This function can be used when the identity of this RPC is not known while creating it."""
        assert self.identity is None
        self.identity = identity
        self.ipc = 'ipc://%s%s.sock' % (ipc_prefix, identity)

    def controller(self):
        next_check = self.client_timeout
        try:
            t = time.time()
            for key, client in self.clients.items():
                diff = self.client_timeout - (t - client.last)
                #print "client", diff
                if diff < 0:
                    del self.clients[key]
                    log.info('%s: Recycling client %s', self.identity, key)
                    client.stop()
                elif diff < next_check:
                    next_check = diff
        finally:
            if self.clients:
                self.control_greenlet = gevent.spawn_later(next_check, self.controller)
            else:
                self.control_greenlet = None

    def decorate_function(self, func, name):
        class Func(object):
            __name__ = name
            __doc__ = func.__doc__

            def __call__(s, *args, **kwargs):
                return func(*args, **kwargs)

            def spawn(s, target, *args, **kwargs):
                return self.spawn(target, name, *args, **kwargs)

            def spawn_safe(s, target, *args, **kwargs):
                return self.spawn_safe(target, name, *args, **kwargs)

            def run(s, target, *args, **kwargs):
                return self.run(target, name, *args, **kwargs)

            def execute(s, target, *args, **kwargs):
                return self.execute(target, name, *args, **kwargs)

            def stream(s, target, *args, **kwargs):
                for data in self.stream(target, name, *args, **kwargs):
                    yield data

            def stream_sync(s, target, *args, **kwargs):
                for data in self.stream_sync(target, name, *args, **kwargs):
                    yield data

        return Func()

    def get_client_address(self, target):
        return target

    def spawn(self, target, name, *args, **kwargs):
        """Call function and give never a response. Fire and forget
        Request only method."""
        return self._call('spawn', target, name, args, kwargs)

    def spawn_safe(self, target, name, *args, **kwargs):
        """Call function and give a response when the message is arrived at the server. This is basically like the `run` function.
        Request with immediately response method."""
        return self._call('spawn_safe', target, name, args, kwargs)

    def run(self, target, name, *args, **kwargs):
        """Call function and wait for response. The server is ignoring connection loss to client.
        Heartbeat method."""
        return self._call('run', target, name, args, kwargs)

    def execute(self, target, name, *args, **kwargs):
        """Call function and wait for response. Interrupt the call on the server if the client connection is lost.
        Ping pong method."""
        return self._call('execute', target, name, args, kwargs)

    def stream(self, target, name, *args, **kwargs):
        """Call function and wait for response, then start the generator. Interrupt the call on the server if the client connection is lost.
        Ping pong method."""
        for data in self._call('stream', target, name, args, kwargs):
            yield data

    def stream_sync(self, target, name, *args, **kwargs):
        """Call function and wait for response, then start the generator. Interrupt the call on the server if the client connection is lost.
        Sends the next packet only when the receive is acknowledged.
        Ping pong method."""
        for data in self._call('stream_sync', target, name, args, kwargs):
            yield data

    def _call(self, call_func, target, name, args, kwargs):
        # Get the target
        if target is not None:
            target = self.get_client_address(target)
        if target is None:
            target = self.ipc
        else:
            target = 'tcp://%s:%d' % (target, self.port)

        # Get a client instance
        if target not in self.clients:
            self.clients[target] = AZRPCClient(self, target)
            log.info('%s: Created new client with connection to %s', self.identity, target)
            if self.control_greenlet is None:
                self.control_greenlet = gevent.spawn_later(self.client_timeout, self.controller)
        client = self.clients[target]

        # Send the request
        return getattr(client, call_func)(name, *args, **kwargs)


class BaseServer(object):
    identity = None
    control_greenlet = None

    def __init__(self, rpc):
        assert rpc.identity
        self.rpc = rpc
        self.messages = dict()
        self.greenlet = gevent.spawn(self.loop)

    def controller(self):
        next_check = self.rpc.heartbeat_timeout
        try:
            t = time.time()
            for msg in self.messages.values():
                if msg.last_recv is not None:
                    # Check heartbeat timeout
                    diff = self.rpc.heartbeat_timeout - (t - msg.last_recv)
                    if diff < 0:
                        msg.greenlet.kill(exception=AZRPCTimeout(self.rpc.heartbeat_timeout))
                        continue
                    elif diff < next_check:
                        next_check = diff
                if msg.heartbeat_msg is not None:
                    # Send heartbeat/ping
                    diff = self.rpc.heartbeat_interval - (t - msg.last_send)
                    if diff < 0:
                        msg.send([b'', msg.uuid, msg.heartbeat_msg, DAT_RAW, b''])
                        diff = self.rpc.heartbeat_interval
                    if diff < next_check:
                        next_check = diff
        finally:
            if self.messages:
                next_check = max(0.1, next_check)
                self.control_greenlet = gevent.spawn_later(next_check, self.controller)
            else:
                self.control_greenlet = None

    def loop(self):
        while True:
            try:
                identity = list()
                while True:
                    msg = self.router.recv()
                    if msg == b'':
                        break
                    identity.append(msg)
                msg = self.router.recv_multipart()
                log.debug('%s: >>> %s -> %s', self.rpc.identity, self.identity, identity + [b''] + msg)
                gevent.spawn(ServerMessage, self, identity, *msg)
            except GreenletExit:
                raise
            except Exception:
                log.exception('%s: Error running at RPC server', self.rpc.identity)

    def execute(self, name, args, kwargs):
        func = self.rpc.get_function(name)
        return func(*args, **kwargs)

class ServerMessage(object):
    """A message that is handled by a server"""

    heartbeat_msg = None
    last_send = None
    last_recv = None

    def __init__(self, server, identity, uuid, msg_type, data_type, data):
        self.server = server
        self.identity = identity
        self.uuid = uuid
        self.greenlet = gevent.getcurrent()

        if msg_type in (CLI_SPAWN, CLI_SPAWN_SAFE):
            if msg_type == CLI_SPAWN_SAFE:
                self.send([b'', self.uuid, SRV_STARTED, DAT_RAW, b''])
            assert data_type == DAT_PICKLE, ('Invalid data type', data_type)
            self.spawn(*cPickle.loads(data))
            return

        elif msg_type in (CLI_RUN, CLI_EXECUTE, CLI_STREAM, CLI_STREAM_SYNC):
            self.last_send = 0
            if msg_type == CLI_RUN:
                self.heartbeat_msg = SRV_HEARTBEAT
            else:
                if msg_type == CLI_STREAM_SYNC:
                    self.acknowledged_queue = Queue()
                self.heartbeat_msg = SRV_PING
                self.last_recv = time.time()
            try:
                self.server.messages[self.uuid] = self
                if self.server.control_greenlet is None:
                    self.server.control_greenlet = gevent.spawn_later(self.server.rpc.heartbeat_interval, self.server.controller)
                assert data_type == DAT_PICKLE, ('Invalid data type', data_type)
                self.run(msg_type, *cPickle.loads(data))
            finally:
                del self.server.messages[self.uuid]

        elif msg_type in (CLI_PONG, CLI_ACKNOWLEDGED, CLI_CANCEL):
            try:
                msg = self.server.messages[self.uuid]
            except KeyError:
                log.info('%s: Missing message for %r %s', self.server.rpc.identity, msg_type, self.uuid.encode('hex'))
            else:
                msg.last_recv = time.time()
                if msg_type == CLI_ACKNOWLEDGED:
                    msg.acknowledged_queue.put(None)
                elif msg_type == CLI_CANCEL:
                    msg.greenlet.kill()

        else:
            log.warning('%s: Unknown message type: %s', self.server.rpc.identity, msg_type)

    def send(self, msg):
        msg = self.identity + msg
        log.debug('%s: <<< %s -> %s', self.server.rpc.identity, self.server.identity, msg)
        self.server.router.send_multipart(msg)
        #self.last_send = time.time()

    def spawn(self, func, args, kwargs):
        try:
            self.server.execute(func, args, kwargs)
        except GreenletExit:
            raise
        except Exception:
            log.exception('%s: Exception in call %s(%s, %s)', self.server.rpc.identity, func, args, kwargs)

    def run(self, msg_type, func, args, kwargs):
        try:
            result = self.server.execute(func, args, kwargs)
            if msg_type == CLI_STREAM:
                if not isinstance(result, GeneratorType):
                    raise RuntimeError('Function must be a generator')
        except Exception as result:
            log.exception('Failed calling %r(%r, %r): %r', func, args, kwargs, result)
            cmd = SRV_ERROR
        else:
            if msg_type == CLI_STREAM:
                cmd = SRV_STREAM
            elif msg_type == CLI_STREAM_SYNC:
                cmd = SRV_STREAM_SYNC
            else:
                cmd = SRV_OK

        if cmd in (SRV_STREAM, SRV_STREAM_SYNC):
            try:
                for data in result:
                    if isinstance(data, basestring):
                        data_type = DAT_RAW
                    else:
                        data_type = DAT_PICKLE
                        data = cPickle.dumps(data, 2)
                    self.send([b'', self.uuid, cmd, data_type, data])
                    if cmd == SRV_STREAM_SYNC:
                        try:
                            self.acknowledged_queue.get()
                        except AZRPCTimeout:
                            break
            finally:
                self.send([b'', self.uuid, SRV_CANCEL, DAT_RAW, b''])
        else:
            if isinstance(result, basestring):
                data_type = DAT_RAW
            else:
                data_type = DAT_PICKLE
                result = cPickle.dumps(result, 2)
            self.send([b'', self.uuid, cmd, data_type, result])


def _check_ipc_socket(rpc, ipc):
    path = ipc[6:]
    if os.path.exists(path):
        log.warning('%s: Deleting IPC socket %s', rpc.identity, path)
        os.unlink(path)


class AZRPCServer(BaseServer):
    """Single RPC instance"""

    def __init__(self, rpc):
        _check_ipc_socket(rpc, rpc.ipc)
        self.identity = '%s-server' % rpc.identity
        self.router = ctx.socket(zmq.ROUTER)
        self.router.setsockopt(zmq.LINGER, 0)
        self.router.setsockopt(zmq.IDENTITY, self.identity)
        self.router.bind(rpc.ipc)
        if rpc.port:
            self.router.bind('tcp://*:%d' % rpc.port)
        super(AZRPCServer, self).__init__(rpc)

class AZRPCWorker(BaseServer):
    """One of many RPC worker instances"""

    def __init__(self, rpc, worker_id):
        self.identity = '%s-worker-%s' % (rpc.identity, worker_id)
        self.router = ctx.socket(zmq.ROUTER)
        self.router.setsockopt(zmq.LINGER, 0)
        self.router.setsockopt(zmq.IDENTITY, self.identity)
        self.router.connect('ipc://%s%s-worker.sock' % (ipc_prefix, rpc.identity))
        super(AZRPCWorker, self).__init__(rpc)


class AZRPCLoadbalancer(object):
    """Single RPC load balancer"""

    def __init__(self, rpc):
        assert rpc.identity
        _check_ipc_socket(rpc, rpc.ipc)

        backend_ipc = 'ipc://%s%s-worker.sock' % (ipc_prefix, rpc.identity)
        _check_ipc_socket(rpc, backend_ipc)

        self.rpc = rpc
        self.identity = '%s-loadbalancer' % rpc.identity
        self.frontend = ctx.socket(zmq.ROUTER)
        self.frontend.setsockopt(zmq.LINGER, 0)
        self.frontend.bind(rpc.ipc)
        if rpc.port:
            self.frontend.bind('tcp://*:%d' % rpc.port)
        self.backend = ctx.socket(zmq.DEALER)
        self.backend.setsockopt(zmq.LINGER, 0)
        self.backend.setsockopt(zmq.IDENTITY, self.identity)
        self.backend.bind(backend_ipc)
        self.greenlet = gevent.spawn(self.loop)

    def loop(self):
        poller = zmq.Poller()
        poller.register(self.backend, zmq.POLLIN)
        poller.register(self.frontend, zmq.POLLIN)
        while True:
            try:
                socks = dict(poller.poll())
                if self.frontend in socks:
                    msg = self.frontend.recv_multipart()
                    log.debug('%s: >>> %s -> %s', self.rpc.identity, self.identity, msg)
                    self.backend.send_multipart(msg)
                if self.backend in socks:
                    msg = self.backend.recv_multipart()
                    log.debug('%s: <<< %s -> %s', self.rpc.identity, self.identity, msg)
                    self.frontend.send_multipart(msg)
            except GreenletExit:
                raise
            except Exception:
                log.exception('%s: Error running at RPC loadbalancer', self.rpc.identity)


class AZRPCClient(object):
    """RPC client class"""

    control_greenlet = None

    def __init__(self, rpc, address='tcp://127.0.0.1:5570'):
        self.rpc = rpc
        self.identity = '%s-client-%s' % (rpc.identity, uuid4().hex)

        self.client = ctx.socket(zmq.DEALER)
        self.client.setsockopt(zmq.LINGER, 0)
        self.client.setsockopt(zmq.IDENTITY, self.identity)
        try:
            self.client.connect(address)
        except Exception as e:
            log.critical('%s: Error connecting to %s: %s', self.rpc.identity, address, e)
            raise

        self.messages = dict()
        self.control_greenlet = None

        self.last = time.time()
        self.greenlet = gevent.spawn(self.loop)

    def stop(self):
        self.greenlet.kill()
        if self.control_greenlet:
            self.control_greenlet.kill()
            self.control_greenlet = None
        for msg in self.messages.values():
            if not msg.result.ready():
                msg.result.set_exception(AZRPCStop())
        self.client.close()

    # Tool and timeout functions

    def _send(self, msg):
        log.debug('%s: <<< %s -> %s', self.rpc.identity, self.identity, msg)
        self.client.send_multipart(msg)
        self.last = time.time()

    def _call(self, msg_type, func, args, kwargs):
        msg = ClientMessage(self)
        self.messages[msg.uuid] = msg
        data = cPickle.dumps([func, args, kwargs], 2)
        self._send([b'', msg.uuid, msg_type, DAT_PICKLE, data])
        return msg

    def _get_result(self, msg, remove_message=True):
        if self.control_greenlet is None:
            self.control_greenlet = gevent.spawn_later(self.rpc.heartbeat_timeout, self.controller)
        try:
            return msg.result.get()
        finally:
            if remove_message:
                del self.messages[msg.uuid]

    # call functions

    def spawn(self, func, *args, **kwargs):
        data = cPickle.dumps([func, args, kwargs], 2)
        self._send([b'', b'', CLI_SPAWN, DAT_PICKLE, data])

    def spawn_safe(self, func, *args, **kwargs):
        msg = self._call(CLI_SPAWN_SAFE, func, args, kwargs)
        return self._get_result(msg)

    def run(self, func, *args, **kwargs):
        msg = self._call(CLI_RUN, func, args, kwargs)
        return self._get_result(msg)

    def execute(self, func, *args, **kwargs):
        msg = self._call(CLI_EXECUTE, func, args, kwargs)
        return self._get_result(msg)

    def _stream(self, cli_cmd, func, args, kwargs):
        msg = self._call(cli_cmd, func, args, kwargs)
        try:
            self._get_result(msg, remove_message=False)
            cmd = None
            try:
                while True:
                    cmd, data = msg.queue.get()
                    self.last = time.time()
                    if cmd == SRV_CANCEL:
                        if isinstance(data, AZRPCTimeout):
                            raise data
                        break
                    yield data
                    if cli_cmd == CLI_STREAM_SYNC:
                        self._send([b'', msg.uuid, CLI_ACKNOWLEDGED, DAT_RAW, b''])
            finally:
                if cmd != SRV_CANCEL:
                    self._send([b'', msg.uuid, CLI_CANCEL, DAT_RAW, b''])
        finally:
            del self.messages[msg.uuid]

    def stream(self, func, *args, **kwargs):
        for value in self._stream(CLI_STREAM, func, args, kwargs):
            yield value

    def stream_sync(self, func, *args, **kwargs):
        for value in self._stream(CLI_STREAM_SYNC, func, args, kwargs):
            yield value

    # engine

    def controller(self):
        next_check = self.rpc.heartbeat_timeout
        try:
            t = time.time()
            for msg in self.messages.values():
                diff = self.rpc.heartbeat_timeout - (t - msg.last)
                if diff < 0:
                    if not msg.result.ready():
                        msg.result.set_exception(AZRPCTimeout(self.rpc.heartbeat_timeout))
                    if msg.queue is not None:
                        msg.queue.put((SRV_CANCEL, AZRPCTimeout(self.rpc.heartbeat_timeout)))
                elif diff < next_check:
                    next_check = diff
        finally:
            if self.messages:
                self.control_greenlet = gevent.spawn_later(next_check, self.controller)
            else:
                self.control_greenlet = None

    def _unserialize(self, data_type, data):
        if data_type == DAT_RAW:
            return data
        elif data_type == DAT_PICKLE:
            return cPickle.loads(data)
        else:
            assert False, ('Invalid data type', data_type)

    def loop(self):
        while True:
            try:
                _, uuid, cmd, data_type, data = self.client.recv_multipart()
                self.last = time.time()
                try:
                    msg = self.messages[uuid]
                except KeyError:
                    if cmd != SRV_CANCEL:
                        self.on_missing_message(uuid)
                else:
                    msg.last = time.time()
                    if cmd == SRV_HEARTBEAT:
                        assert data_type == DAT_RAW, ('Invalid data type', data_type)
                    elif cmd == SRV_PING:
                        assert data_type == DAT_RAW, ('Invalid data type', data_type)
                        self._send([b'', uuid, CLI_PONG, DAT_RAW, b''])
                    elif cmd == SRV_STARTED:
                        assert data_type == DAT_RAW, ('Invalid data type', data_type)
                        msg.result.set()
                    elif cmd == SRV_OK:
                        data = self._unserialize(data_type, data)
                        msg.result.set(data)
                    elif cmd == SRV_ERROR:
                        data = self._unserialize(data_type, data)
                        msg.result.set_exception(data)
                    elif cmd in (SRV_STREAM, SRV_STREAM_SYNC):
                        data = self._unserialize(data_type, data)
                        msg.init_stream()
                        msg.queue.put((cmd, data))
                    elif cmd == SRV_CANCEL:
                        assert data_type == DAT_RAW, ('Invalid data type', data_type)
                        msg.init_stream()
                        msg.queue.put((SRV_CANCEL, None))
                    else:
                        self.on_unknown_control_message(cmd)
            except GreenletExit:
                raise
            except Exception:
                log.exception('%s: Error running at RPC client', self.rpc.identity)

    def on_missing_message(self, uuid):
        log.warning('%s: Missing message for result %s', self.rpc.identity, uuid.encode('hex'))

    def on_unknown_control_message(self, cmd):
        log.warning('%s: Unknown control message received: %s (ord: %i)', self.rpc.identity, cmd.encode('hex'), ord(cmd[0]))

class ClientMessage(object):
    """RPC client message that is waiting for a result"""

    queue = None

    def __init__(self, client):
        self.client = client
        self.uuid = uuid4().bytes
        self.last = time.time()
        self.result = AsyncResult()

    def init_stream(self):
        if self.queue is None:
            self.result.set(None)
            self.queue = Queue()


# Test

def main():
    from gevent.monkey import patch_all
    patch_all()

    import sys

    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.DEBUG)

    rpc = AZRPC('test')

    @rpc.register
    def foo1(a, b):
        print "foo1 a", a, b
        gevent.sleep(0.2)
        print "foo1 b", a, b
        return a + b

    @rpc.register
    def foo2(a, b):
        print "foo2", a, b
        return a*b

    @rpc.register
    def foo2spawned(a, b):
        print "foo2spawned", a, b
        return a*b

    @rpc.register
    def foo2stream(a, b):
        for i in xrange(5):
            yield "foo2spawned", a, b, i
            gevent.sleep(0.1)

    @rpc.register
    def sleep(a):
        try:
            print "sleep", a
            gevent.sleep(a)
        except Exception:
            log.exception('Sleep exception')

    rpc.add(gevent.sleep, 'sleep')

    if sys.argv[1] == 'server':
        AZRPCServer(rpc)
        gevent.wait()
    elif sys.argv[1] == 'worker':
        AZRPCWorker(rpc, sys.argv[2])
        gevent.wait()
    elif sys.argv[1] == 'loadbalancer':
        AZRPCLoadbalancer(rpc)
        gevent.wait()
    elif sys.argv[1] == 'client':
        print "stream"
        for x in foo2stream.stream(None, 2, 3):
            print x
        print "spawn"
        print foo2spawned.spawn(None, 2, 3)
        print sleep.spawn(None, 5)
        print "spawn_safe"
        print sleep.spawn_safe(None, 5)
        print "run"
        print foo1.run(None, 2, 3)
        print rpc.run(None, '__main__.foo1', 2, 3)
        print foo2.run(None, 2, 3)
        print "run"
        print sleep.run(None, 10)
        print "execute"
        print sleep.execute(None, 10)
        print "spawn_safe"
        print sleep.spawn_safe(None, 5)
        print "done"
        gevent.sleep(100)
    else:
        print "unknown role", sys.argv[1]

if __name__ == '__main__':
    main()
