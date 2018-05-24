from voltdb import FastSerializer, VoltProcedure
import json, uuid, datetime
import re

def get_wkp():
    regex = r"[^\s]*\s*([\d]*)/[^\s]*.*"
    regex = re.compile(regex)

    ports = []
    with open ("/etc/services", "r") as f:
        for line in f:
            m = regex.match(line)
            if m:
                ports.append(int(m.group(1)))
    
    return ports

def populate_dbn(client, data):
    pass


def populate_active_connections(client, data):
    TABLENAME = "Active_Connection"
    proc = VoltProcedure(client, TABLENAME+".insert", [FastSerializer.VOLTTYPE_TIMESTAMP,
        FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_INTEGER, FastSerializer.VOLTTYPE_INTEGER])
    for d in data:
        dt = datetime.datetime.fromtimestamp(d["ts"])
        response = proc.call([dt, str(d.get("srcIP", "")), str(d.get("dstIP", "")), int(d.get("srcPort", "")), int(d.get("dstPort", "")), str(d.get("srcIP", ""))])

def populate_ADV_table(client, data):
    TABLENAME = "Average_Data_Volume"
    proc = VoltProcedure(client, "UpsertAverageDataVolume", [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_INTEGER])
    for d in data:
        response = proc.call([str(d["srcIP"]), str(d["dstIP"]), d["size"]])

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
            time = d["ts"]+d["ms"]/10000.0
            time = datetime.datetime.fromtimestamp(time)
            hour = datetime.datetime(time.year, time.month, time.day, time.hour)
            response = proc.call([hour, time, flag])

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

def populate_wkp(client, data):
    proc = VoltProcedure(client, "WELL_KNOWN_PORTS.insert", [
            FastSerializer.VOLTTYPE_STRING,
            FastSerializer.VOLTTYPE_INTEGER
        ])

    wkp = get_wkp()
    for d in data:
        dst_port = int(d.get("dstPort", ""))

        if dst_port in wkp:
            res = proc.call([
                str(d.get("dstIP", "")),
                dst_port
            ])


def populate_ip_connections(client, data):
    TABLENAME = "IP_CONNECTIONS"
    proc = VoltProcedure(client, TABLENAME+".insert", [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_INTEGER, FastSerializer.VOLTTYPE_STRING])
    for d in data:
        response = proc.call([str(d.get("dstIP", "")), int(d.get("dstPort", "")), str(d.get("srcIP", ""))])

def populateAll(client, data):
    d = data
    proc = VoltProcedure(client, "InsertPacket", [FastSerializer.VOLTTYPE_TIMESTAMP, FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_INTEGER, FastSerializer.VOLTTYPE_INTEGER, FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_INTEGER])

    time = d["ts"]+d["ms"]/10000.0
    time = datetime.datetime.fromtimestamp(time)
    srcIP = str(d.get("srcIP", ""))
    dstIP = str(d.get("dstIP", ""))
    srcPort = int(d.get("srcPort", ""))
    dstPort = int(d.get("dstPort", ""))
    flag = ""
    if d["SYN"]:
        flag = "S"
    if d["FIN"]:
        flag = "F"
    payload = str(d.get("payload", ""))
    pType = str(d.get("dstIP", ""))
    size = int(d.get("size",""))
    
    print proc.call([time, srcIP, dstIP, srcPort, dstPort, flag, payload, pType, size])
    
with open ("resources/tcpdump_large.json", "r") as f:
    client = FastSerializer("192.168.58.3", 21212)

    objects = ijson.items(f, 'item')
    for obj in objects:
        populateAll(client, obj)

    client.close()
