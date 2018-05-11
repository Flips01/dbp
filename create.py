import json

def populate_dbn(data):
    pass

with open ("resources/tcpdump.json", "r") as f:
    data=f.read()
    j_data = json.loads(data)
    populate_dbn(j_data)
