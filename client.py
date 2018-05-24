import sys
import client_queries

def select_query():
    def _m():
        print "------------------------------------"
        print "Select a Query:"
        print "[1] All active connections at a certain point in time"
        print "[2] Average data volume for all connections between IP a.b.c.d and IP w.x.y.z"
        print "[3] All hosts that had connections to IP a.b.c.d on a given port number"
        print "[4] All hosts that had incoming connections on well-known ports"
        print "[5] All packets that contain a given byte sequence"
        print "[6] Ratio of SYN packets to FIN packets in a given time period"
        print "[exit] Exit Program"
        print "------------------------------------"

    client_queries._clear()
    while True:
        _m()
        qry = raw_input("Query selection: ")
        qry = qry.strip()

        if qry not in ["1", "2", "3", "4", "5", "6", "exit"]:
            client_queries._clear()
            print "Invalid Input!"
            continue

        if qry == "exit":
            sys.exit()

        return qry


while True:
    qry = select_query()

    client_queries._clear()
    if qry == "1":
        client_queries.query1()
		if qry == "2":
				client_queries.query2()
		if qry == "3":
				client_queries.query3()				
		if qry == "4":
				client_queries.query4()
		if qry == "5":
				client_queries.query5()
		if qry == "6":
				client_queries.query6()
    raw_input("Press Enter to continue")
