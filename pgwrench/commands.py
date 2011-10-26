
from pgwrench import sequence, data
from pgwrench import acl

def list_sequences(db, options, args):
  print "Listing sequences"
  print

  opts={}
  if options.schema:
    opts['schema_name']=options.schema
  if options.table:
    opts['table_name']=options.table

  seqs = sequence.list_sequences(db,**opts)

  schema = None
  for seq in seqs:
    if seq['schema_name'] != schema:
      schema = seq['schema_name']
      print "\nSchema %s\n" % schema

    print "  %(table_name)s.%(column_name)s" % seq
    print "    seq name:         %(seq_name)s" % seq

    if seq["has_acl_mismatch"]:
      print "    acl mismatch:     table : %(table_acl)s " % seq
      print "                      seq   : %(seq_acl)s " % seq

    if seq["has_offset"]:
      print "    offset:           offset:           %(seq_offset)12d" % seq
      print "                      seq value:        %(seq_last)12d" % seq
      print "                      max column value: %(max_value)12d" % seq
    print


def fix_sequences_values(db, options, args):
  print "Fixing sequences values"
  print

  opts={}
  if options.schema:
    opts['schema_name']=options.schema
  if options.table:
    opts['table_name']=options.table

  sequence.fix_sequences_values(db,**opts)
  print "done"


def set_sequences_permissions(db, options, args):
  print "Setting sequences permissions"
  print

  opts={}
  if options.schema:
    opts['schema_name']=options.schema
  if options.table:
    opts['table_name']=options.table

  sequence.set_permissions(db,**opts)
  print "done"


def data_upsert(db, options, args):
    print "-- creating update/insert statements"
    print

    data.generate_upserts(db, options, args)
    print "-- done"
