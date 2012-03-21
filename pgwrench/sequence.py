
import psycopg2.extras
from pgwrench import acl

def list_sequences(db, schema_name = None, table_name = None, problematic_only=True):
  """
  list all sequences
  """
  cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
  sql = """select pns.nspname as schema_name,
        pc.relname as table_name,
        pc.relacl as table_acl,
        pg_get_userbyid(pc.relowner) as table_owner,
        pa.attname as column_name,
        pc2.relname as seq_name,
        pns2.nspname as seq_schema_name,
        pc2.relacl as seq_acl,
        pg_get_userbyid(pc2.relowner) as seq_owner
      from pg_catalog.pg_attrdef pad
      join pg_catalog.pg_class pc on pad.adrelid=pc.oid
      join pg_catalog.pg_namespace pns on pns.oid = pc.relnamespace
      join pg_catalog.pg_attribute pa on pa.attrelid = pc.oid and pad.adnum = pa.attnum
      join pg_catalog.pg_depend dep on pad.oid = dep.objid
      join pg_catalog.pg_class pc2 on pc2.oid = dep.refobjid and pc2.relkind='S'
      join pg_catalog.pg_namespace pns2 on pns2.oid = pc2.relnamespace"""

  wheres=[]
  if schema_name:
    if type(schema_name) == str:
      wheres.append("pns.nspname = '%s'" % schema_name)
    if type(schema_name) == list:
      wheres.append("pns.nspname in ('%s')" % "','".join(schema_name))

  if table_name:
    if type(table_name) == str:
      wheress.append("pc.relname = '%s'" % table_name)
    if type(table_name) == list:
      wheres.append("pc.relname in ('%s')" % "','".join(table_name))

  if len(wheres)>0:
    sql += " where " + " and ".join(wheres)

  sql += " order by pns.nspname, pc.relname, pa.attname"

  cur.execute(sql)
  sequences = cur.fetchall()
  seqs=[]

  for i in range(len(sequences)):
    seq = dict(sequences[i])

    # set the acl objects
    seq["seq_acl"] = acl.AclDict(seq["seq_acl"])
    seq["table_acl"] = acl.AclDict(seq["table_acl"])

    # get the value of the sequence and the table
    params = seq
    params['seqfull'] = seq['seq_name']
    if params['seqfull'].find(".") == -1:
      params['seqfull'] = '"' + seq['seq_schema_name'] + '"."' + params['seqfull'] + '"'
      seq['seq_name'] = params['seqfull']

    cur.execute("""
      select seq_last, max_value, seq_last-max_value as seq_offset
      from (
        select seq.last_value as seq_last,
               (select coalesce(max("%(column_name)s"), 0) from "%(schema_name)s"."%(table_name)s") as max_value
        from  %(seqfull)s seq
      ) foo""" % params)

    seqvals = cur.fetchone()
    seq.update(dict(seqvals))

    # find problems
    seq["has_acl_mismatch"] = seq["seq_acl"] != acl.seq_acl_from_table(seq["table_acl"])
    # do not modify sequences of empty tables
    seq["has_offset"] = seq['seq_offset'] not in (None, 0) and (seq['seq_last']==1 and seq['max_value']!=0)

    if problematic_only:
      if seq["has_offset"] or seq["has_acl_mismatch"]:
        seqs.append(seq)
    else:
      seqs.append(seq)


  return seqs


def fix_sequences_values(db, fix_negative_offset = True, fix_positive_offset=True ,**kwargs):
  sequences = list_sequences(db, **kwargs)
  cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

  for seq in sequences:
    fix = False
    if seq["has_offset"]:

      if seq["seq_offset"] < 0 and fix_negative_offset:
        fix = True
      if seq["seq_offset"] > 0 and fix_positive_offset:
        fix = True

    if fix:
      seq_value = seq['max_value'] or 1
      cur.execute("select setval('%s', %d, true)" % (seq['seq_name'], seq_value))
  db.commit()


def set_permissions(db, **kwargs):
  sequences = list_sequences(db, **kwargs)
  cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

  for seq in sequences:
    if seq["has_acl_mismatch"]:
      new_seq_acl = acl.seq_acl_from_table(seq["table_acl"])

      for sql in seq["seq_acl"].revoke("sequence", seq["seq_name"]):
        print sql
        cur.execute(sql)

      for sql in new_seq_acl.grant("sequence", seq["seq_name"]):
        print sql
        cur.execute(sql)

  db.commit()
