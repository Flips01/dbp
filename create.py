from voltdb import FastSerializer, VoltProcedure
import json

def populate_dbn(client, data):
    pass

with open ("resources/tcpdump.json", "r") as f:
    client = FastSerializer("192.168.58.3", 21212)

    data=f.read()
    j_data = json.loads(data)
    populate_dbn(j_data)

    client.close()