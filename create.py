import datetime, threading, math, time, itertools
from Queue import Queue

try:
    import ijson.backends.yajl2 as ijson
except ImportError:
    import ijson

SERVERS = ["192.168.58.2","192.168.58.3","192.168.58.4","192.168.58.5"]

########################################################################################################################
########################################################################################################################
########################################################################################################################

class JsonDataReader(threading.Thread):
    def __init__(self, file, batch_size=10, q_maxsize=250):
        super(JsonDataReader, self).__init__()
        self.file = file
        self.batch_size = batch_size
        self.queue = Queue(q_maxsize)
        self.finished = False

    def is_finished(self):
        return self.finished

    def get_queue(self):
        return self.queue

    def run(self):
        with open(self.file, "r") as f:
            packets = ijson.items(f, 'item')

            batch = []
            for packet in packets:
                batch.append(packet)

                if len(batch) == self.batch_size:
                    self.queue.put(batch)
                    batch = []

            if len(batch) > 0:
                self.queue.put(batch)
        self.finished = True

class ImportWorker(threading.Thread):
    def __init__(self, server, datareader):
        super(ImportWorker, self).__init__()
        self.server = server
        self.datareader = datareader
        self.queue = datareader.get_queue()

        self.processed_packets = 0

    def init_voltdb(self):
        from voltdb import FastSerializer, VoltProcedure
        self.v_client = FastSerializer(self.server, 21212)
        print "making client with %s" % self.server
        self.v_proc = VoltProcedure(self.v_client, "InsertPacket", [FastSerializer.VOLTTYPE_TIMESTAMP, FastSerializer.VOLTTYPE_STRING,
                                                      FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_INTEGER,
                                                      FastSerializer.VOLTTYPE_INTEGER, FastSerializer.VOLTTYPE_STRING,
                                                      FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING,
                                                      FastSerializer.VOLTTYPE_INTEGER])

    def destroy_voltdb(self):
        self.v_client.close()

    def handle_packet(self, packet):
        time = packet["ts"] + packet["ms"] / 10000.0
        time = datetime.datetime.fromtimestamp(time)
        srcIP = str(packet.get("srcIP", ""))
        dstIP = str(packet.get("dstIP", ""))
        srcPort = packet.get("srcPort", None)
        dstPort = packet.get("dstPort", None)
        flag = ""
        if packet["SYN"]:
            flag = "S"
        if packet["FIN"]:
            flag = "F"
        payload = str(packet.get("payload", ""))
        pType = str(packet.get("type", ""))
        size = packet.get("size", 0)
        self.v_proc.call([time, srcIP, dstIP, srcPort, dstPort, flag, payload, pType, size])

    def get_processed_packets(self):
        return self.processed_packets

    def run(self):
        self.init_voltdb()

        while True:
            batch = self.queue.get(True)
            for packet in batch:
                self.handle_packet(packet)
            self.processed_packets += len(batch)

        self.destroy_voltdb()

########################################################################################################################
########################################################################################################################
########################################################################################################################

server_cycle = None
def rr_server():
    global server_cycle

    if not server_cycle:
        server_cycle = itertools.cycle(SERVERS)

    return server_cycle.next()

def clear():
    print("\033[H\033[J")

########################################################################################################################
# RUN
########################################################################################################################

FILE = "/tmp/tcpdump.json"
WORKER_LIMIT = 8
WORKERS = []

dr = JsonDataReader(FILE)
dr.start()

# worker starten
for i in range(0, WORKER_LIMIT):
    server = str(rr_server())
    worker = ImportWorker(server, dr)
    worker.start()

    WORKERS.append(worker)


while True:
    clear()

    # Wurden alle Daten eingelesen?
    if dr.is_finished():
        # warten bis alle items verarbeitet wurden
        while not dr.queue.empty():
            time.sleep(1)
        # alle worker stoppen
        for worker in WORKERS:
            pass
        break

    print("Worker Status:")
    print("-"*10)
    for i in range(0, len(WORKERS)):
        print("Worker #%s: %s" % (i, WORKERS[i].get_processed_packets()))
    print("-" * 10)
    print("Queued packets: %s" % dr.get_queue().qsize())
    time.sleep(1)

print("Imported Completed!")