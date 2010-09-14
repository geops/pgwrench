About
-----

pgwrench is a utility for some administrative task in Postgresql databases.

At the moment it offers commands th handle primary keys with a default value
provided by a sequence.


Usage
-----

The following is the output of pgwrench when run with "--help":


  Usage:
    usage: pgwrench.py [options] command

    primarykey sequences
    --------------------

      The commands in this section are used on sequences which are set as the
      default value of a column in a table. This is for example the case with
      the "serial" datatype.

      pkseq find          : list all sequences where one of the following problems
                            exists:
                              - the max. value of the column and the current
                                value of the sequence are not the same.
                                This will propably cause foreign key violations when
                                inserting new rows into this table when the column is used as
                                primary key.
                              - the permissions on the table and sequence do not match.
                                A user might no be able to insert new rows although
                                the user has the neccessary permissions on the table.
                                The cause for this are missing permissions on the sequence
                                to fetch a new primary key.

      pkseq fix-values    : set the current value of the sequence to the
                            max. value of the table column where it is used
                            to get the default value.

      pkseq set-acl       : set the permissions of the sequence according to the
                            permissions of the table.


  Options:
    -h, --help            show this help message and exit
    -n SCHEMA, --schema=SCHEMA
    -t TABLE, --table=TABLE
    -H HOSTNAME, --host=HOSTNAME
                          name of db host
    -d DBNAME, --dbname=DBNAME
                          name of database to connect to
    -p PORT, --port=PORT  port of the database server
    -U NAME, --username=NAME
                          username to use for login on the database server
    -w, --no-password     Do not ask for pwassword


Requirements
------------

* python 2.5+
* psycopg2
