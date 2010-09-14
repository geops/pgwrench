
ACL_MAPPING={
  "a": "INSERT",       # means "append"
  "r": "SELECT",       # means "read"
  "w": "UPDATE",       # means "write"
  "d": "DELETE",
  "D": "TRUNCATE",
  "x": "REFERENCES",
  "C": "CREATE",
  "c": "CONNECT",
  "T": "TEMPORARY",
  "U": "USAGE",
  "t": "TRIGGER"
}

class AclItem(object):

  def __init__(self, aclitemstr=None):
    self.user = None
    self.perms = set()
    self.granted_by = None

    if aclitemstr:
      if aclitemstr.find("/") != -1:
        aclitemstr = aclitemstr[:aclitemstr.find("/")]
        self.granted_by = aclitemstr[aclitemstr.find("/"):].strip("/")

      user, perms = aclitemstr.split("=")
      if user == '':
        self.user = "public"
      else:
        self.user = user

      for cr in perms:
        if ACL_MAPPING.has_key(cr):
          self.perms.add(ACL_MAPPING[cr])

  @property
  def is_empty(self):
    return self.perms == set()

  @property
  def sql_perms(self):
    return ", ".join(self.perms)

  def __repr__(self):
    acls = ""
    for k,v in ACL_MAPPING.iteritems():
      if v in self.perms:
        acls += k
    return "%s=%s" % (self.user, acls)

  def __eq__(self, other):
    return self.user == other.user and self.perms == other.perms


class AclDict(dict):

  def __init__(self, aclstr=None):
    if aclstr:
      aclstr = aclstr.strip("{}")
      aclitemstrs = aclstr.split(',')

      for aclitemstr in aclitemstrs:
        aclitem = AclItem(aclitemstr)
        self[aclitem.user] = aclitem

  def revoke(self, reltype, relname):
    """
    retuns a list of sqls to revoke al permissions
    """
    sqls = []
    for aclitem in self.values():
      sqls.append("revoke all on %s %s from %s" % (reltype, relname, aclitem.user))
    return sqls


  def grant(self, reltype, relname):
    """
    returns a list of sql to grant all permissions
    """
    sqls = []
    for aclitem in self.values():
      sqls.append("grant %s on %s %s to %s" % (aclitem.sql_perms, reltype, relname, aclitem.user))
    return sqls

  def __repr__(self):
    return ",".join(map(repr, self.values()))

def seq_acl_from_table(acldict):
  """
  get the corresponding sequences acls based on the acls
  of a table
  """
  seqacl = AclDict()

  for tacl in acldict.values():
    sacl = AclItem()
    sacl.user = tacl.user
    if "SELECT" in tacl.perms:
      sacl.perms.add("SELECT")
    if "INSERT" in tacl.perms:
      sacl.perms.add("USAGE")
      sacl.perms.add("UPDATE")

    if not sacl.is_empty:
      seqacl[sacl.user] = sacl
  return seqacl
