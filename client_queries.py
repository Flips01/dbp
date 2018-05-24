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


def second_query():
    pass
