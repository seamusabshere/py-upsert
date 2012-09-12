class Upsert:
  def __init__(self, connection, table_name):
    self.connection = connection
    self.table_name = table_name

  def row(self, selector):
    sql = "INSERT INTO %s (%s) VALUES ('%s')"  % (self.table_name, selector.keys()[0], selector.values()[0])
    cur = self.connection.cursor()
    cur.execute(sql)
