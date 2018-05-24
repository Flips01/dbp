import random, datetime
from voltdb import FastSerializer, VoltProcedure

def get_voltdb_client():
    servers = ["192.168.58.2","192.168.58.3","192.168.58.4","192.168.58.5"]
    return FastSerializer(random.choice(servers), 21212)

def _clear():
    print("\033[H\033[J")

def first_query():
    dt = None
    
    while not dt:
        d_str = raw_input("Enter Date [DD.MM.YYYY HH:MM]: ")
        
        try:
            dt = datetime.datetime.strptime(d_str, "%d.%m.%Y %H:%M")
        except:
            _clear()
            print "Invalid Input-Format!"

    client = get_voltdb_client()

    client.close()


def query1():
    pass

def query2():
	pass

def query3():
	ip_str = raw_input("Enter IP in dotted Notation")
  port_int = raw_input("Enter Port as single Number")
	client = get_voltdb_client()
	proc = VoltProcedure(client, "query3", [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_INTEGER]).call([ip_str, port_int])
	client.close()

def query4():
	client = get_voltdb_client()
  proc = VoltProcedure(client, "WELL_KNOWN_PORTS.select").call()
  client.close()

def query5():
  pass

def query6():
  pass
