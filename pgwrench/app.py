import sys

from optparse import OptionParser
import getpass

import psycopg2

from pgwrench import commands



version="0.2"

def cmdline_parser():

  usage = """
  usage: %prog [options] command

  primarykey sequences:
    pkseq find          : list all sequences which are used for default values
                          where one of the following problems exists:
                            * the max. value of the column and the current
                              value of the sequence are off
                            * the owner of the table and sequence do not match

    pkseq fix-values    : set the current value of the sequence to the
                          max. value of the table column where it is used
                          to get the default value.

    pkseq set-acl       : set the permissions of the sequence accoring to the 
                          permissions of the table.
  """

  parser = OptionParser(usage)

  # operation options
  parser.add_option("-n", "--schema", dest="schema",
    help="", metavar="SCHEMA")
  parser.add_option("-t", "--table", dest="table",
    help="", metavar="TABLE")

  # connection options
  parser.add_option("-H", "--host", dest="host",
    help="name of db host", metavar="HOSTNAME")
  parser.add_option("-d", "--dbname", dest="dbname",
    help="name of database to connect to", metavar="DBNAME")
  parser.add_option("-p", "--port", dest="port",
    help="port of the database server", metavar="PORT")
  parser.add_option("-U", "--username", dest="username",
    help="username to use for login on the database server",
    metavar="NAME")
  parser.add_option("-w", "--no-password", action="store_true",
    dest="no_password",help="Do not ask for pwassword")
  return parser.parse_args()


def cmdline_err(msg):
  print("%s. see --help" % msg)
  sys.exit(1)


def connect_db(options):
  dbstr = ""
  if options.username:
    dbstr += ' user=%s' % options.username
  if options.port:
    dbstr += ' port=%s' % options.port
  if options.dbname:
    dbstr += ' dbname=%s' % options.dbname
  if not options.no_password:
    dbstr += " password='%s'" % getpass.getpass()
  if options.host:
    dbstr += " host=%s" % options.host

  return psycopg2.connect(dbstr)



def run():
  options, args = cmdline_parser()

  if len(args)==0:
    cmdline_err("no command specfified")

  try:
    db = connect_db(options)
  except psycopg2.OperationalError,e :
    print e.message
    sys.exit(1)

  cmd = args[0].lower()
  if cmd in ("sequences", "seq", "pkseq"):
    if len(args)<2:
      cmdline_err("please specifiy what to do")

    subcmd = args[1].lower()
    if subcmd in ("find", "list"):
      commands.list_sequences(db, options, args)
    elif subcmd in ("fix", "fix-values"):
      commands.fix_sequences_values(db, options, args)
    elif subcmd == "set-acl":
      commands.set_sequences_permissions(db, options, args)
    else:
      cmdline_err("unknown subcommand")

  else:
    cmdline_err("unknown command")
