
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
      print "Schema %s" % schema
      print

    print "\t%(table_name)s.%(column_name)s\t Sequence: %(seq_name)s" % seq 
    print "\t\tSeq Value: %(seq_last)d\n\t\tMax Column Value: %(max_value)s\n\t\tOffset: %(seq_offset)s" % seq
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
