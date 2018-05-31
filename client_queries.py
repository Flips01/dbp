import random, datetime
from voltdb import FastSerializer, VoltProcedure

def get_voltdb_client():
    servers = ["192.168.58.2","192.168.58.3","192.168.58.4","192.168.58.5"]
    return FastSerializer(random.choice(servers), 21212)

def _clear():
    print("\033[H\033[J")
    pass

def query1():
    dt = getCorrectDate("Enter Time [DD.MM.YYYY HH:MM]: ")

    client = get_voltdb_client()
    
    proc = VoltProcedure(client, "SelectActiveConnections", [FastSerializer.VOLTTYPE_TIMESTAMP])
    print proc.call([dt])
    client.close()
    pass        
    
def query2():
    print "Retrieve the average data volume for all connections between IP a.b.c.d and IP w.x.y.z"
    ip_A = str(raw_input("Enter IP a.b.c.d in dotted Notation: "))
    ip_B = str(raw_input("Enter IP w.x.y.z in dotted Notation: "))

    client = get_voltdb_client()
    proc = VoltProcedure(client, "SelectAverageDataVolume", [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING])
    print proc.call([ip_A, ip_B])
    pass

def query3():
    ip_str = str(raw_input("Enter IP in dotted Notation: "))
    port_int = int(raw_input("Enter Port as single Number: "))
    client = get_voltdb_client()
    proc = VoltProcedure(client, "query3", [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_INTEGER])
    print proc.call([ip_str, port_int])
    client.close()
    pass

def query4():
    port_int = int(raw_input("Enter Port as single Number: "))
    client = get_voltdb_client()
    proc = VoltProcedure(client, "WELL_KNOWN_PORTS.select",[FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_INTEGER])
    print proc.call(["*",port_int])
    client.close()
    pass

def query5():
    #All packets that contain a given byte sequence

    sequence = raw_input("Enter byte sequence: ")

    client = get_voltdb_client()
    proc = VoltProcedure(client, "SelectByteSequence",[FastSerializer.VOLTTYPE_STRING])
    print proc.call([sequence])
    client.close()

def query6():

    startTime = getCorrectDate("Enter Start Time [DD.MM.YYYY HH:MM]: ")
    endTime = getCorrectDate("Enter End Time [DD.MM.YYYY HH:MM]: ")

    client = get_voltdb_client()
    proc = VoltProcedure(client, "SynFinRatio",[FastSerializer.VOLTTYPE_TIMESTAMP, FastSerializer.VOLTTYPE_TIMESTAMP])
    print proc.call([startTime,endTime])
    client.close()

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