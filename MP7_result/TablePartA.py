import happybase as hb

connection = hb.Connection()

powers_families = {
    'personal': {},
    'professional': {},
    'custom': {},
}

food_families = {
    'nutrition': {},
    'taste': {},
}

connection.create_table('powers', powers_families)
connection.create_table('food', food_families)

connection.close()