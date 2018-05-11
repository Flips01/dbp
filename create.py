from voltdb import FastSerializer, VoltProcedure
import json, uuid, datetime

def populate_packets(client, data):
    proc = VoltProcedure(client, "PACKET.insert", [
            FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING,
            FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_INTEGER,
            FastSerializer.VOLTTYPE_INTEGER, FastSerializer.VOLTTYPE_STRING,
            FastSerializer.VOLTTYPE_TIMESTAMP, FastSerializer.VOLTTYPE_STRING,
            FastSerializer.VOLTTYPE_STRING
        ])

    for d in data:
        flag = ""
        if d["SYN"]:
            flag = "s"
        if d["FIN"]:
            flag = "f"
        
        dt = datetime.datetime.fromtimestamp(d["ts"])
        res = proc.call([
            str(uuid.uuid4()), str(d.get("srcIP", "")), str(d.get("dstIP", "")),
            int(d.get("srcPort", "")), int(d.get("dstPort", "")), str(d.get("payload", "")),
            dt, flag, str(d.get("type", "")) 
        ])

def populate_dbn(client, data):
    pass

with open ("resources/tcpdump.json", "r") as f:
    client = FastSerializer("192.168.58.3", 21212)

    data=f.read()
    j_data = json.loads(data)
    populate_dbn(client, j_data)
    populate_packets(client, j_data)

    client.close()