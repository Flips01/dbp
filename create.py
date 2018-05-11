from voltdb import FastSerializer, VoltProcedure
import json

def populate_dbn(client, data):
    pass

def populate_ADV_table(client, data):
    TABLENAME = "Average_Data_Volume"
    proc = VoltProcedure(client, TABLENAME+".insert", [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_INTEGER, FastSerializer.VOLTTYPE_INTEGER])
    for d in data:
        response = proc.call([d["srcIP"], d["dstIP"], 1, d["size"]])


with open ("resources/tcpdump.json", "r") as f:
    client = FastSerializer("192.168.58.3", 21212)

    data=f.read()
    j_data = json.loads(data)
    populate_dbn(client, j_data)

    client.close()
