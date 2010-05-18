import sys

from optparse import OptionParser
import getpass

import psycopg2

from pgwrench import commands

def cmdline_parser():

  usage = """
  usage: %prog [options] command

    sequences list
    sequences fix
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
  if cmd == "sequences":
    if len(args)<2:
      cmdline_err("please specifiy what to do")
    
    subcmd = args[1].lower()
    if subcmd == "list":
      commands.list_sequences(db, options, args)
    elif subcmd == "fix":
      commands.fix_sequences(db, options, args)


  else:
    cmdline_err("unknow command")
