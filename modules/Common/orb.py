# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

import threading
import socket
import json

"""Object Request Broker

This module implements the infrastructure needed to transparently create
objects that communicate via networks. This infrastructure consists of:

--  Strub ::
        Represents the image of a remote object on the local machine.
        Used to connect to remote objects. Also called Proxy.
--  Skeleton ::
        Used to listen to incoming connections and forward them to the
        main object.
--  Peer ::
        Class that implements basic bidirectional (Stub/Skeleton)
        communication. Any object wishing to transparently interact with
        remote objects should extend this class.

"""


class ComunicationError(Exception):
    pass


class Stub(object):
    """ Stub for generic objects distributed over the network.

    This is  wrapper object for a socket.

    """

    def __init__(self, address):
        self.address = tuple(address)

    def send(self, message):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect(self.address)
            message = message + "\n"
            self.socket.send(message.encode())
            result = self.socket.recv(1024)
        finally:
            self.socket.close();
        return result

    def _rmi(self, method, *args):
        #
        # Your code here.
        #
        try:
            message = json.dumps({"method":method,"args":args})
            result = self.send(message)
            output = json.loads(result.decode())
            if "error" in output:
                exception = type(output["error"]["name"], (Exception,), {})
                raise exception(output["error"]["args"])
            return output["result"]
        finally:
            pass
        
    
    def __getattr__(self, attr):
        """Forward call to name over the network at the given address."""
        def rmi_call(*args):
            return self._rmi(attr, *args)
        return rmi_call


class Request(threading.Thread):
    """Run the incoming requests on the owner object of the skeleton."""

    def __init__(self, owner, conn, addr):
        threading.Thread.__init__(self)
        self.addr = addr
        self.conn = conn
        self.owner = owner
        self.daemon = True

    # Tries to process the request, sends back an exception to the client if its raised.
    def process_request(self, request):
        try:
            message = json.loads(request)
            fn = getattr(self.owner, message["method"])
            return json.dumps({"result" : fn(*message["args"])})
        except Exception as e:
            exec_name = e.__class__.__name__
            exec_args = e.args
            print(exec_name)
            print(e)
            result = json.dumps({"error": {"name": exec_name, "args": exec_args}})
            return result

    def run(self):
        try:
            # Threat the socket as a file stream.
            worker = self.conn.makefile(mode="rw")
            # Read the request in a serialized form (JSON).
            request = worker.readline()
            # print(request)
            # Process the request.
            result = self.process_request(request)
            # Send the result.
            worker.write(result + '\n')
            worker.flush()
        except Exception as e:
            # Catch all errors in order to prevent the object from crashing
            # due to bad connections coming from outside.
            print("The connection to the caller has died:")
            print("\t{}: {}".format(type(e), e))
        finally:
            self.conn.close()


class Skeleton(threading.Thread):
    """ Skeleton class for a generic owner.

    This is used to listen to an address of the network, manage incoming
    connections and forward calls to the generic owner class.

    """

    def __init__(self, owner, address):
        threading.Thread.__init__(self)
        self.address = address
        self.owner = owner
        self.daemon = True
        #
        # Your code here.
        #
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(self.address)
        self.server.listen(1)


    def run(self):
        #
        # Your code here.
        #
        try:
            while True:
                try:
                    conn, addr = self.server.accept()
                    req = Request(self.owner, conn, addr)
                    # print("\n" + "Serving a request from {0}".format(addr))
                    req.start()
                except socket.error:
                    continue
        except KeyboardInterrupt:
            pass


class Peer:
    """Class, extended by objects that communicate over the network."""

    def __init__(self, l_address, ns_address, ptype):
        self.type = ptype
        self.hash = ""
        self.id = -1
        self.address = self._get_external_interface(l_address)
        self.skeleton = Skeleton(self, self.address)
        self.name_service_address = self._get_external_interface(ns_address)
        self.name_service = Stub(self.name_service_address)

    # Private methods

    def _get_external_interface(self, address):
        """ Determine the external interface associated with a host name.

        This function translates the machine's host name into its the
        machine's external address, not into '127.0.0.1'.

        """

        addr_name = address[0]
        if addr_name != "":
            addrs = socket.gethostbyname_ex(addr_name)[2]
            if len(addrs) == 0:
                raise ComunicationError("Invalid address to listen to")
            elif len(addrs) == 1:
                addr_name = addrs[0]
            else:
                al = [a for a in addrs if a != "127.0.0.1"]
                addr_name = al[0]
        addr = list(address)
        addr[0] = addr_name
        return tuple(addr)

    # Public methods

    def start(self):
        """Start the communication interface."""

        self.skeleton.start()
        self.id, self.hash = self.name_service.register(self.type,
                                                        self.address)

    def destroy(self):
        """Unregister the object before removal."""

        self.name_service.unregister(self.id, self.type, self.hash)

    def check(self):
        """Checking to see if the object is still alive."""
        # print("What ever I want")
        return (self.id, self.type)
