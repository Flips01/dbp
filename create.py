from voltdb import FastSerializer, VoltProcedure
import json, uuid, datetime

def populate_dbn(client, data):
    pass

def populate_ADV_table(client, data):
    TABLENAME = "Average_Data_Volume"
    proc = VoltProcedure(client, TABLENAME+".insert", [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_INTEGER, FastSerializer.VOLTTYPE_INTEGER])
    for d in data:
        response = proc.call([d["srcIP"], d["dstIP"], 1, d["size"]])

def populate_SYNFIN_table(client, data):
    TABLENAME = "SYN_FIN_RATIO"
    proc = VoltProcedure(client, TABLENAME+".insert", [FastSerializer.VOLTTYPE_TIMESTAMP, FastSerializer.VOLTTYPE_TIMESTAMP, FastSerializer.VOLTTYPE_STRING])
    for d in data:
        if d["FIN"] or d["SYN"]:
            flag = ""
            if d["FIN"]:
                flag = "F"
            elif d["SYN"]:
                flag = "S"
            time = d["ts"]+d["ms"]/10000
            response = proc.call([datetime.datetime.fromTimestamp((time//(1000*60*60))*1000*60*60), datetime.datetime.fromTimestamp(time), flag])

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



with open ("resources/tcpdump.json", "r") as f:
    client = FastSerializer("192.168.58.3", 21212)

    data=f.read()
    j_data = json.loads(data)
    populate_dbn(client, j_data)
    populate_packets(client, j_data)
    populate_SYNFIN_table(client,j_data)
    client.close()