#!/usr/bin/env python3

# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Server that serves clients trying to work with the database."""

import threading
import socket
import json
import random
import argparse

import sys
sys.path.append("../modules")
from Server.database import Database
from Server.Lock.readWriteLock import ReadWriteLock

# -----------------------------------------------------------------------------
# Initialize and read the command line arguments
# -----------------------------------------------------------------------------

rand = random.Random()
rand.seed()
description = """\
Server for a fortune database. It allows clients to access the database in
parallel.\
"""
parser = argparse.ArgumentParser(description=description)
parser.add_argument(
    "-p", "--port", metavar="PORT", dest="port", type=int,
    default=rand.randint(1, 10000) + 40000, choices=range(40001, 50000),
    help="Set the port to listen to. Values in [40001, 50000]. "
         "The default value is chosen at random."
)
parser.add_argument(
    "-f", "--file", metavar="FILE", dest="file", default="dbs/fortune.db",
    help="Set the database file. Default: dbs/fortune.db."
)
opts = parser.parse_args()

db_file = opts.file
server_address = ("", opts.port)

# -----------------------------------------------------------------------------
# Auxiliary classes
# -----------------------------------------------------------------------------


class Server(object):
    """Class that provides synchronous access to the database."""

    def __init__(self, db_file):
        self.db = Database(db_file)
        self.rwlock = ReadWriteLock()

    # Public methods
    def read(self):
        #
        # Your code here.
        #

        # Acquire the lock before reading, in order to
        # prevent any writing operations during read.
        self.rwlock.read_acquire()
        
        try: 
            result = self.db.read()
        finally:
            # Release lock and return result when we are done.
            self.rwlock.read_release()
        return result

    def write(self, fortune):
        #
        # Your code here
        #
        
        # Acquire the lock when writing, in order for no
        # reading or writing from other threads to perform
        # any operations during this write. Lock is released 
        # afterwards, and default value "NULL" is returned to
        # indicate that everything went as it should.
        self.rwlock.write_acquire()
        try:
            self.db.write(fortune)
        finally:
            self.rwlock.write_release()
        return "NULL"


class Request(threading.Thread):
    """ Class for handling incoming requests.
        Each request is handled in a separate thread.
    """

    def __init__(self, db_server, conn, addr):
        threading.Thread.__init__(self)
        self.db_server = db_server
        self.conn = conn
        self.addr = addr
        self.daemon = True

    # Private methods

    def process_request(self, request):
        """ Process a JSON formated request, send it to the database, and
            return the result.

            The request format is:
                {
                    "method": called_method_name,
                    "args": called_method_arguments
                }

            The returned result is a JSON of the following format:
                -- in case of no error:
                    {
                        "result": called_method_result
                    }
                -- in case of error:
                    {
                        "error": {
                            "name": error_class_name,
                            "args": error_arguments
                        }
                    }
        """
        #
        # Your code here.
        #
        
        # Decode the message recieved. This may cause an
        # error if the sent message being sent is in fact 
        # not a JSON object. Then, an exeption indicating 
        # this is returned.
        #  try:
        #     message = json.loads(request)
        # except ValueError as e:
        #   result = json.dumps({"error":{"name":"Unknown communication protocol exception","args": [""]}})
        #  return result

        # result = ""
        # getaddr(objct, name)
        # a list -> A list of arguments
        # list tunpacking python
        # Pack exceptiosn extracting name and arguments of eception thrown
        try :
            message = json.loads(request)
            fn = getattr(self.db_server, message["method"])
            result = json.dumps({"result" : fn(*message["args"])})

            # if("method" in message):
            #fn = get_attr(db_server, message["method"])
            # fn(*message["args"]);
            
            """ if(message["method"] == "read"):
            result = self.db_server.read()
            result = json.dumps({"result":result})
            elif(message["method"] == "write"):
            if("args" in message and message["args"] != ""):
            result = self.db_server.write(message["args"])
            result = json.dumps({"result":result})
            else:
            #Throw arguments missing exception
            result = json.dumps({"error":{"name":"Arguments missing exception","args": [message["method"]]}})
            else:
            #Throw undefined method exception
            result = json.dumps({"error":{"name":"Undefined method exception","args": [message["method"]]}})"""
            # else:
            # Throw unknown communication protocol exception
            #    result = json.dumps({"error":{"name":"Unknown communication protocol exception","args": [""]}})
            
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

# -----------------------------------------------------------------------------
# The main program
# -----------------------------------------------------------------------------

print("Listening to: {}:{}".format(socket.gethostname(), opts.port))
with open("srv_address.tmp", "w") as f:
    f.write("{}:{}\n".format(socket.gethostname(), opts.port))

sync_db = Server(db_file)

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(server_address)
server.listen(1)

print("Press Ctrl-C to stop the server...")

try:
    while True:
        try:
            conn, addr = server.accept()
            req = Request(sync_db, conn, addr)
            print("Serving a request from {0}".format(addr))
            req.start()
        except socket.error:
            continue
except KeyboardInterrupt:
    pass
