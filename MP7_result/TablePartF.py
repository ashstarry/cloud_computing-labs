import happybase as hb

connection = hb.Connection()
powers_table = connection.table('powers')
# DON'T CHANGE THE PRINT FORMAT, WHICH IS THE OUTPUT
# OR YOU WON'T RECEIVE POINTS FROM THE GRADER


powers_scan = []

for key, data in powers_table.scan():
    powers_scan.append(data)

# print(powers_scan)

for data in powers_scan:
    for data1 in powers_scan:
        color1 = data[b'custom:color']
        name1 = data[b'professional:name']
        power1 = data[b'personal:power']

        color2 = data1[b'custom:color']
        name2 = data1[b'professional:name']
        power2 = data1[b'personal:power']
        
        if color1 == color2 and name1 != name2:
            print('{}, {}, {}, {}, {}'.format(name1, power1, name2, power2, color1))

connection.close()

