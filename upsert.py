class Upsert:
  def __init__(self, connection, table_name):
    self.connection = connection
    self.table_name = table_name

  def row(self, selector):
    template = 'INSERT INTO "%s" ("%s")' % (self.table_name, selector.keys()[0])
    cur = self.connection.cursor()
    cur.execute((template + " VALUES (%s)"), (selector.values()[0],))
