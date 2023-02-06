import happybase as hb
connection = hb.Connection()
powers_table = connection.table('powers')
# DON'T CHANGE THE PRINT FORMAT, WHICH IS THE OUTPUT
# OR YOU WON'T RECEIVE POINTS FROM THE GRADER

for idx, data in powers_table.scan(include_timestamp=True):
    print('Found: {}, {}'.format(idx, data))

connection.close()