"""Base implementation of event loop.

The event loop can be broken up into a multiplexer (the part
responsible for notifying us of IO events) and the event loop proper,
which wraps a multiplexer with functionality for scheduling callbacks,
immediately or at a given time in the future.

Whenever a public API takes a callback, subsequent positional
arguments will be passed to the callback if/when it is called.  This
avoids the proliferation of trivial lambdas implementing closures.
Keyword arguments for the callback are not supported; this is a
conscious design decision, leaving the door open for keyword arguments
to modify the meaning of the API call itself.
"""


import collections
import concurrent.futures
import socket
import subprocess
import os
import sys

from asyncio import events
from asyncio import futures
from asyncio import tasks
from asyncio.base_events import Server
import traceback

from .log import logger

__all__ = ['BaseEventLoop']


# Argument for default thread pool executor creation.
_MAX_WORKERS = 5


class BaseEventLoop(events.AbstractEventLoop):

    def __init__(self):
        self._default_executor = None
        self._internal_fds = 0
        self._debug = (not sys.flags.ignore_environment
                       and bool(os.environ.get('PYTHONASYNCIODEBUG')))
        self._exception_handler = None
        self._current_handle = None

    def get_debug(self):
        return self._debug

    def set_debug(self, enabled):
        self._debug = enabled

    def create_task(self, coro):
        """Schedule a coroutine object.

        Return a task object.
        """
        task = tasks.Task(coro, loop=self)
        if task._source_traceback:
            del task._source_traceback[-1]
        return task

    def _make_socket_transport(self, sock, protocol, waiter=None, *,
                               extra=None, server=None):
        """Create socket transport."""
        raise NotImplementedError

    def _make_ssl_transport(self, rawsock, protocol, sslcontext, waiter, *,
                            server_side=False, server_hostname=None,
                            extra=None, server=None):
        """Create SSL transport."""
        raise NotImplementedError

    def _make_datagram_transport(self, sock, protocol,
                                 address=None, extra=None):
        """Create datagram transport."""
        raise NotImplementedError

    def _make_read_pipe_transport(self, pipe, protocol, waiter=None,
                                  extra=None):
        """Create read pipe transport."""
        raise NotImplementedError

    def _make_write_pipe_transport(self, pipe, protocol, waiter=None,
                                   extra=None):
        """Create write pipe transport."""
        raise NotImplementedError

    @tasks.coroutine
    def _make_subprocess_transport(self, protocol, args, shell,
                                   stdin, stdout, stderr, bufsize,
                                   extra=None, **kwargs):
        """Create subprocess transport."""
        raise NotImplementedError

    def _read_from_self(self):
        """XXX"""
        raise NotImplementedError

    def _write_to_self(self):
        """XXX"""
        raise NotImplementedError

    def close(self):
        """Close the event loop.

        This clears the queues and shuts down the executor,
        but does not wait for the executor to finish.
        """
        executor = self._default_executor
        if executor is not None:
            self._default_executor = None
            executor.shutdown(wait=False)

    def call_soon_threadsafe(self, callback, *args):
        """XXX"""
        handle = self.call_soon(callback, *args)
        self._write_to_self()
        return handle

    def run_in_executor(self, executor, callback, *args):
        if isinstance(callback, events.Handle):
            assert not args
            assert not isinstance(callback, events.TimerHandle)
            if callback._cancelled:
                f = futures.Future(loop=self)
                f.set_result(None)
                return f
            callback, args = callback._callback, callback._args
        if executor is None:
            executor = self._default_executor
            if executor is None:
                executor = concurrent.futures.ThreadPoolExecutor(_MAX_WORKERS)
                self._default_executor = executor
        return futures.wrap_future(executor.submit(callback, *args), loop=self)

    def set_default_executor(self, executor):
        self._default_executor = executor

    def getaddrinfo(self, host, port, *,
                    family=0, type=0, proto=0, flags=0):
        return self.run_in_executor(None, socket.getaddrinfo,
                                    host, port, family, type, proto, flags)

    def getnameinfo(self, sockaddr, flags=0):
        return self.run_in_executor(None, socket.getnameinfo, sockaddr, flags)

    @tasks.coroutine
    def create_connection(self, protocol_factory, host=None, port=None, *,
                          ssl=None, family=0, proto=0, flags=0, sock=None,
                          local_addr=None, server_hostname=None):
        """XXX"""
        if server_hostname is not None and not ssl:
            raise ValueError('server_hostname is only meaningful with ssl')

        if server_hostname is None and ssl:
            # Use host as default for server_hostname.  It is an error
            # if host is empty or not set, e.g. when an
            # already-connected socket was passed or when only a port
            # is given.  To avoid this error, you can pass
            # server_hostname='' -- this will bypass the hostname
            # check.  (This also means that if host is a numeric
            # IP/IPv6 address, we will attempt to verify that exact
            # address; this will probably fail, but it is possible to
            # create a certificate for a specific IP address, so we
            # don't judge it here.)
            if not host:
                raise ValueError('You must set server_hostname '
                                 'when using ssl without a host')
            server_hostname = host

        if host is not None or port is not None:
            if sock is not None:
                raise ValueError(
                    'host/port and sock can not be specified at the same time')

            f1 = self.getaddrinfo(
                host, port, family=family,
                type=socket.SOCK_STREAM, proto=proto, flags=flags)
            fs = [f1]
            if local_addr is not None:
                f2 = self.getaddrinfo(
                    *local_addr, family=family,
                    type=socket.SOCK_STREAM, proto=proto, flags=flags)
                fs.append(f2)
            else:
                f2 = None

            yield from tasks.wait(fs, loop=self)

            infos = f1.result()
            if not infos:
                raise OSError('getaddrinfo() returned empty list')
            if f2 is not None:
                laddr_infos = f2.result()
                if not laddr_infos:
                    raise OSError('getaddrinfo() returned empty list')

            exceptions = []
            for family, type, proto, cname, address in infos:
                try:
                    sock = socket.socket(family=family, type=type, proto=proto)
                    sock.setblocking(False)
                    if f2 is not None:
                        for _, _, _, _, laddr in laddr_infos:
                            try:
                                sock.bind(laddr)
                                break
                            except OSError as exc:
                                exc = OSError(
                                    exc.errno, 'error while '
                                    'attempting to bind on address '
                                    '{!r}: {}'.format(
                                        laddr, exc.strerror.lower()))
                                exceptions.append(exc)
                        else:
                            sock.close()
                            sock = None
                            continue
                    yield from self.sock_connect(sock, address)
                except OSError as exc:
                    if sock is not None:
                        sock.close()
                    exceptions.append(exc)
                else:
                    break
            else:
                if len(exceptions) == 1:
                    raise exceptions[0]
                else:
                    # If they all have the same str(), raise one.
                    model = str(exceptions[0])
                    if all(str(exc) == model for exc in exceptions):
                        raise exceptions[0]
                    # Raise a combined exception so the user can see all
                    # the various error messages.
                    raise OSError('Multiple exceptions: {}'.format(
                        ', '.join(str(exc) for exc in exceptions)))

        elif sock is None:
            raise ValueError(
                'host and port was not specified and no sock specified')

        sock.setblocking(False)

        protocol = protocol_factory()
        waiter = futures.Future(loop=self)
        if ssl:
            sslcontext = None if isinstance(ssl, bool) else ssl
            transport = self._make_ssl_transport(
                sock, protocol, sslcontext, waiter,
                server_side=False, server_hostname=server_hostname)
        else:
            transport = self._make_socket_transport(sock, protocol, waiter)

        yield from waiter
        return transport, protocol

    @tasks.coroutine
    def create_datagram_endpoint(self, protocol_factory,
                                 local_addr=None, remote_addr=None, *,
                                 family=0, proto=0, flags=0):
        """Create datagram connection."""
        if not (local_addr or remote_addr):
            if family == 0:
                raise ValueError('unexpected address family')
            addr_pairs_info = (((family, proto), (None, None)),)
        else:
            # join addresss by (family, protocol)
            addr_infos = collections.OrderedDict()
            for idx, addr in ((0, local_addr), (1, remote_addr)):
                if addr is not None:
                    assert isinstance(addr, tuple) and len(addr) == 2, (
                        '2-tuple is expected')

                    infos = yield from self.getaddrinfo(
                        *addr, family=family, type=socket.SOCK_DGRAM,
                        proto=proto, flags=flags)
                    if not infos:
                        raise OSError('getaddrinfo() returned empty list')

                    for fam, _, pro, _, address in infos:
                        key = (fam, pro)
                        if key not in addr_infos:
                            addr_infos[key] = [None, None]
                        addr_infos[key][idx] = address

            # each addr has to have info for each (family, proto) pair
            addr_pairs_info = [
                (aikey, addr_pair) for aikey, addr_pair in addr_infos.items()
                if not ((local_addr and addr_pair[0] is None) or
                        (remote_addr and addr_pair[1] is None))]

            if not addr_pairs_info:
                raise ValueError('can not get address information')

        exceptions = []

        for ((family, proto),
             (local_address, remote_address)) in addr_pairs_info:
            sock = None
            r_addr = None
            try:
                sock = socket.socket(
                    family=family, type=socket.SOCK_DGRAM, proto=proto)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.setblocking(False)

                if local_addr:
                    sock.bind(local_address)
                if remote_addr:
                    yield from self.sock_connect(sock, remote_address)
                    r_addr = remote_address
            except OSError as exc:
                if sock is not None:
                    sock.close()
                exceptions.append(exc)
            else:
                break
        else:
            raise exceptions[0]

        protocol = protocol_factory()
        transport = self._make_datagram_transport(sock, protocol, r_addr)
        return transport, protocol

    @tasks.coroutine
    def create_server(self, protocol_factory, host=None, port=None,
                      *,
                      family=socket.AF_UNSPEC,
                      flags=socket.AI_PASSIVE,
                      sock=None,
                      backlog=100,
                      ssl=None,
                      reuse_address=None):
        """XXX"""
        if isinstance(ssl, bool):
            raise TypeError('ssl argument must be an SSLContext or None')
        if host is not None or port is not None:
            if sock is not None:
                raise ValueError(
                    'host/port and sock can not be specified at the same time')

            AF_INET6 = getattr(socket, 'AF_INET6', 0)
            if reuse_address is None:
                reuse_address = os.name == 'posix' and sys.platform != 'cygwin'
            sockets = []
            if host == '':
                host = None

            infos = yield from self.getaddrinfo(
                host, port, family=family,
                type=socket.SOCK_STREAM, proto=0, flags=flags)
            if not infos:
                raise OSError('getaddrinfo() returned empty list')

            completed = False
            try:
                for res in infos:
                    af, socktype, proto, canonname, sa = res
                    try:
                        sock = socket.socket(af, socktype, proto)
                    except socket.error:
                        # Assume it's a bad family/type/protocol combination.
                        continue
                    sockets.append(sock)
                    if reuse_address:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,
                                        True)
                    # Disable IPv4/IPv6 dual stack support (enabled by
                    # default on Linux) which makes a single socket
                    # listen on both address families.
                    if af == AF_INET6 and hasattr(socket, 'IPPROTO_IPV6'):
                        sock.setsockopt(socket.IPPROTO_IPV6,
                                        socket.IPV6_V6ONLY,
                                        True)
                    try:
                        sock.bind(sa)
                    except OSError as err:
                        raise OSError(err.errno, 'error while attempting '
                                      'to bind on address %r: %s'
                                      % (sa, err.strerror.lower()))
                completed = True
            finally:
                if not completed:
                    for sock in sockets:
                        sock.close()
        else:
            if sock is None:
                raise ValueError(
                    'host and port was not specified and no sock specified')
            sockets = [sock]

        server = Server(self, sockets)
        for sock in sockets:
            sock.listen(backlog)
            sock.setblocking(False)
            self._start_serving(protocol_factory, sock, ssl, server)
        return server

    @tasks.coroutine
    def connect_read_pipe(self, protocol_factory, pipe):
        protocol = protocol_factory()
        waiter = futures.Future(loop=self)
        transport = self._make_read_pipe_transport(pipe, protocol, waiter)
        yield from waiter
        return transport, protocol

    @tasks.coroutine
    def connect_write_pipe(self, protocol_factory, pipe):
        protocol = protocol_factory()
        waiter = futures.Future(loop=self)
        transport = self._make_write_pipe_transport(pipe, protocol, waiter)
        yield from waiter
        return transport, protocol

    @tasks.coroutine
    def subprocess_shell(self, protocol_factory, cmd, *, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         universal_newlines=False, shell=True, bufsize=0,
                         **kwargs):
        if not isinstance(cmd, str):
            raise ValueError("cmd must be a string")
        if universal_newlines:
            raise ValueError("universal_newlines must be False")
        if not shell:
            raise ValueError("shell must be True")
        if bufsize != 0:
            raise ValueError("bufsize must be 0")
        protocol = protocol_factory()
        transport = yield from self._make_subprocess_transport(
            protocol, cmd, True, stdin, stdout, stderr, bufsize, **kwargs)
        return transport, protocol

    @tasks.coroutine
    def subprocess_exec(self, protocol_factory, *args, stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        universal_newlines=False, shell=False, bufsize=0,
                        **kwargs):
        if universal_newlines:
            raise ValueError("universal_newlines must be False")
        if shell:
            raise ValueError("shell must be False")
        if bufsize != 0:
            raise ValueError("bufsize must be 0")
        protocol = protocol_factory()
        transport = yield from self._make_subprocess_transport(
            protocol, args, False, stdin, stdout, stderr, bufsize, **kwargs)
        return transport, protocol

    def set_exception_handler(self, handler):
        """Set handler as the new event loop exception handler.

        If handler is None, the default exception handler will
        be set.

        If handler is a callable object, it should have a
        signature matching '(loop, context)', where 'loop'
        will be a reference to the active event loop, 'context'
        will be a dict object (see `call_exception_handler()`
        documentation for details about context).
        """
        if handler is not None and not callable(handler):
            raise TypeError('A callable object or None is expected, '
                            'got {!r}'.format(handler))
        self._exception_handler = handler

    def default_exception_handler(self, context):
        """Default exception handler.

        This is called when an exception occurs and no exception
        handler is set, and can be called by a custom exception
        handler that wants to defer to the default behavior.

        The context parameter has the same meaning as in
        `call_exception_handler()`.
        """
        message = context.get('message')
        if not message:
            message = 'Unhandled exception in event loop'

        exception = context.get('exception')
        if exception is not None:
            exc_info = (type(exception), exception, exception.__traceback__)
        else:
            exc_info = False

        if ('source_traceback' not in context
        and self._current_handle is not None
        and self._current_handle._source_traceback):
            context['handle_traceback'] = self._current_handle._source_traceback

        log_lines = [message]
        for key in sorted(context):
            if key in {'message', 'exception'}:
                continue
            value = context[key]
            if key == 'source_traceback':
                tb = ''.join(traceback.format_list(value))
                value = 'Object created at (most recent call last):\n'
                value += tb.rstrip()
            elif key == 'handle_traceback':
                tb = ''.join(traceback.format_list(value))
                value = 'Handle created at (most recent call last):\n'
                value += tb.rstrip()
            else:
                value = repr(value)
            log_lines.append('{}: {}'.format(key, value))

        logger.error('\n'.join(log_lines), exc_info=exc_info)

    def call_exception_handler(self, context):
        """Call the current event loop's exception handler.

        The context argument is a dict containing the following keys:

        - 'message': Error message;
        - 'exception' (optional): Exception object;
        - 'future' (optional): Future instance;
        - 'handle' (optional): Handle instance;
        - 'protocol' (optional): Protocol instance;
        - 'transport' (optional): Transport instance;
        - 'socket' (optional): Socket instance.

        New keys maybe introduced in the future.

        Note: do not overload this method in an event loop subclass.
        For custom exception handling, use the
        `set_exception_handler()` method.
        """
        if self._exception_handler is None:
            try:
                self.default_exception_handler(context)
            except Exception:
                # Second protection layer for unexpected errors
                # in the default implementation, as well as for subclassed
                # event loops with overloaded "default_exception_handler".
                logger.error('Exception in default exception handler',
                             exc_info=True)
        else:
            try:
                self._exception_handler(self, context)
            except Exception as exc:
                # Exception in the user set custom exception handler.
                try:
                    # Let's try default handler.
                    self.default_exception_handler({
                        'message': 'Unhandled error in exception handler',
                        'exception': exc,
                        'context': context,
                    })
                except Exception:
                    # Guard 'default_exception_handler' in case it is
                    # overloaded.
                    logger.error('Exception in default exception handler '
                                 'while handling an unexpected error '
                                 'in custom exception handler',
                                 exc_info=True)
