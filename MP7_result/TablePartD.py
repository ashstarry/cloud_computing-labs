import happybase as hb
connection = hb.Connection()
powers_table = connection.table('powers')

# DON'T CHANGE THE PRINT FORMAT, WHICH IS THE OUTPUT
# OR YOU WON'T RECEIVE POINTS FROM THE GRADER

# Id: "row1", Values for (hero, power, name, xp, color)
row = powers_table.row('row1')
hero = row[b'personal:hero']
power = row[b'personal:power']
name = row[b'professional:name']
xp = row[b'professional:xp']
color = row[b'custom:color']
print('hero: {}, power: {}, name: {}, xp: {}, color: {}'.format(hero, power, name, xp, color))

# Id: "row19", Values for (hero, color)
row = powers_table.row('row19')
hero = row[b'personal:hero']
color = row[b'custom:color']
print('hero: {}, color: {}'.format(hero, color))

# Id: "row1", Values for (hero, name, color)
row = powers_table.row('row1')
hero = row[b'personal:hero']
name = row[b'professional:name']
color = row[b'custom:color']
print('hero: {}, name: {}, color: {}'.format(hero, name, color))

connection.close()