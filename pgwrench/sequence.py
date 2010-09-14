
import psycopg2.extras

def list_sequences(db, schema_name = None, table_name = None, problematic_only=True):
  """
  list all sequences
  """
  cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

  sql = """select pns.nspname as schema_name,
        pc.relname as table_name,
        pa.attname as column_name,
        substring(pg_catalog.pg_get_expr(pad.adbin, pad.adrelid), $$nextval\(\\'([^\\']*)\\'::regclass\)$$) as seq_name
      from pg_catalog.pg_attrdef pad
      join pg_catalog.pg_class pc on pad.adrelid=pc.oid
      join pg_catalog.pg_namespace pns on pns.oid = pc.relnamespace
      join pg_catalog.pg_attribute pa on pa.attrelid = pc.oid and pad.adnum = pa.attnum
      where
        pad.adsrc ~* $$nextval\(\\'[^\\']*\\'::regclass\)$$"""


  if schema_name:
    if type(schema_name) == str:
      sql += " and  pns.nspname = '%s'" % schema_name
    if type(schema_name) == list:
      sql += " and pns.nspname in ('%s')" % "','".join(schema_name)

  if table_name:
    if type(table_name) == str:
      sql += " and  pc.relname = '%s'" % table_name
    if type(table_name) == list:
      sql += " and pc.relname in ('%s')" % "','".join(table_name)

  sql += " order by pns.nspname, pc.relname, pa.attname"

  cur.execute(sql)
  sequences = cur.fetchall()
  seqs=[]

  for i in range(len(sequences)):
    seq = dict(sequences[i])

    # get the value of the sequence and the table
    params = seq
    params['seqfull'] = seq['seq_name']
    if params['seqfull'].find(".") == -1:
      params['seqfull'] = seq['schema_name'] + "." + params['seqfull']
      seq['seq_name'] = params['seqfull']

    cur.execute("""
      select seq_last, max_value, seq_last-max_value as seq_offset
      from (
        select seq.last_value as seq_last,
               (select max(%(column_name)s) from %(schema_name)s.%(table_name)s) as max_value
        from  %(seqfull)s seq
      ) foo""" % params)

    seqvals = cur.fetchone()
    seq.update(dict(seqvals))


    if problematic_only:
      if seq['seq_offset'] not in (None, 0):
        seqs.append(seq)
    else:
      seqs.append(seq)


  return seqs


def fix_sequences(db, fix_negative_offset = True, fix_positive_offset=True ,**kwargs):

  sequences = list_sequences(db, **kwargs)

  cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

  for seq in sequences:
    fix = False
    if seq["seq_offset"] < 0 and fix_negative_offset:
      fix = True
    if seq["seq_offset"] > 0 and fix_positive_offset:
      fix = True

    if fix:
      cur.execute("select setval('%(seq_name)s', %(max_value)d, true)" % seq)

  db.commit()
