
from pgwrench import sequence

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
    print "    Sequence name:    %(seq_name)s" % seq
    print "    Seq Value:        %(seq_last)12d" % seq
    print "    Max Column Value: %(max_value)12d" % seq
    print "    Offset:           %(seq_offset)12d" % seq
    print


def fix_sequences(db, options, args):
  print "Fixing sequences"
  print

  opts={}
  if options.schema:
    opts['schema_name']=options.schema
  if options.table:
    opts['table_name']=options.table

  sequence.fix_sequences(db,**opts)

  print "done"
