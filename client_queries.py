import random, datetime, math, time
from voltdb import FastSerializer, VoltProcedure

def get_voltdb_client():
    servers = ["192.168.58.2","192.168.58.3","192.168.58.4","192.168.58.5"]
    return FastSerializer(random.choice(servers), 21212)

def _clear():
    print("\033[H\033[J")
    pass

#Do not try this under windows, it won't work. Unix does though
def query1():
    dt = getCorrectDate("Enter Time [DD.MM.YYYY HH:MM]: ")

    dt_min = dt - datetime.timedelta(minutes=1)
    dt_max = dt + datetime.timedelta(minutes=1)

    client = get_voltdb_client()

    partitions = partitionsInRange(dtToTs(dt_min), dtToTs(dt_max))
    for partition in partitions:
        proc = VoltProcedure(client, "SelectActiveConnections", [FastSerializer.VOLTTYPE_INTEGER, FastSerializer.VOLTTYPE_TIMESTAMP, FastSerializer.VOLTTYPE_TIMESTAMP])
        res = proc.call([partition, dt_min, dt_max])
        for elem in res.tables[0].tuples:
            print "From %s:%s To %s:%s" % (
                elem[0],elem[1],elem[2],elem[3]
            )

    client.close()

    
def query2():
    print "Retrieve the average data volume for all connections between IP a.b.c.d and IP w.x.y.z"
    ip_A = str(raw_input("Enter IP a.b.c.d in dotted Notation: "))
    ip_B = str(raw_input("Enter IP w.x.y.z in dotted Notation: "))

    client = get_voltdb_client()
    proc = VoltProcedure(client, "SelectAverageDataVolume", [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING])
    result =  proc.call([ip_A, ip_B])
    client.close()
    for elem in result.tables[0].tuples:
        print elem[0]
    pass

def query3():
    ip_str = str(raw_input("Enter IP in dotted Notation: "))
    port_int = int(raw_input("Enter Port as single Number: "))
    client = get_voltdb_client()
    proc = VoltProcedure(client, "query3", [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_INTEGER])
    result = proc.call([ip_str, port_int])
    client.close()
    for elem in result.tables[0].tuples:
        print elem
    pass

def query4():
    port_int = int(raw_input("Enter Port as single Number: "))
    client = get_voltdb_client()
    proc = VoltProcedure(client, "@AdHoc",[FastSerializer.VOLTTYPE_STRING])
    result = proc.call(["SELECT * FROM Well_Known_Ports where DST_PORT = %s"%port_int])
    client.close()
    for elem in result.tables[0].tuples:
        print elem[0]
    pass

def query5():
    #All packets that contain a given byte sequence

    sequence = raw_input("Enter byte sequence: ")

    client = get_voltdb_client()
    proc = VoltProcedure(client, "SelectByteSequence",[FastSerializer.VOLTTYPE_STRING])
    result = proc.call([sequence])
    client.close()
    for elem in result.tables[0].tuples:
        print "Paket %s: %s\n"%(elem[1],elem[2])

def query6():

    startTime = getCorrectDate("Enter Start Time [DD.MM.YYYY HH:MM]: ")
    endTime = getCorrectDate("Enter End Time [DD.MM.YYYY HH:MM]: ")

    client = get_voltdb_client()

    partitions = partitionsInRange(dtToTs(startTime), dtToTs(endTime))

    results = {"F":0, "S":0}

    for partition in partitions:
        proc = VoltProcedure(client, "SynFinRatio", [FastSerializer.VOLTTYPE_INTEGER, FastSerializer.VOLTTYPE_TIMESTAMP, FastSerializer.VOLTTYPE_TIMESTAMP])
        res = proc.call([partition, startTime, endTime])
        for elem in res.tables[0].tuples:
            if elem[1] in results.keys():
                results[elem[1]] += elem[0]

    ratio = 0
    if results["F"] > 0:
        ratio = float(results["S"]) / float(results["F"])
    
    print "Syns: %s; Fins: %s; Ratio: %s" %(
        results["S"], results["F"], ratio
    )

    client.close()

def dtToTs(dt):
    return time.mktime(dt.timetuple())

def getCorrectDate(msg):
    dt = None

    while not dt:
        d_str = raw_input(msg)

        try:
            dt = datetime.datetime.strptime(d_str, "%d.%m.%Y %H:%M")
        except:
            _clear()
            print "Invalid Input-Format!"
    return dt


def partitionsInRange(start, end, partitionInterval = 1800):
    start_part = partitionTs(start, partitionInterval)
    end_part = partitionTs(end, partitionInterval)

    part_range = end_part-start_part
    partitions = math.floor((part_range/partitionInterval)+1)

    parts = []

    for i in range(0, int(partitions)):
        parts.append(int(start_part + (i*partitionInterval)))

    return parts


#Converts Timestamp to Timestamp_Range

def partitionTs(ts,partitionInterval):
    p = math.floor(ts / partitionInterval)
    return p * partitionInterval
