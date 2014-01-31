# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 24 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Implementation of a simple database class."""

import random


class Database(object):
    """Class containing a database implementation."""

    def __init__(self, db_file):
        self.db_file = db_file
        self.rand = random.Random()
        self.rand.seed()
        #
        # Your code here.
        #
        read_line = "";
        self.database = []
        # Read database from file
        with open(self.db_file) as f:
            for line in f:
                if(line == "%\n"):
                    self.database.append(read_line)
                    read_line = ""
                else:
                    read_line += line;
                    
        
        print(len(self.database));

    def read(self):
        """Read a random location in the database."""
        #
        # Your code here.
        #
        # Get Random number
            
        return random.choice(self.database)


    def write(self, fortune):
        """Write a new fortune to the database."""
        #
        # Your code here.
        #
        
        # Append to database
        self.database.append(fortune)

        # Append to files
        with open(self.db_file, "a") as myfile:
            myfile.write(fortune + "\n%\n")

            

