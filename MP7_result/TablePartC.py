import happybase as hb
import csv

connection = hb.Connection()
powers_table = connection.table('powers')

file = open('input.csv')
rows = csv.reader(file)
batch = powers_table.batch()
for row in rows:
    batch.put( row[0], {'personal:hero': row[1], 
                        'personal:power': row[2], 
                        'professional:name': row[3], 
                        'professional:xp': row[4], 
                        'custom:color': row[5]})
batch.send()
connection.close()
file.close()

