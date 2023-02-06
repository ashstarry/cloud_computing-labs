import happybase as hb

connection = hb.Connection()
tables = connection.tables()
print(tables)
connection.close()