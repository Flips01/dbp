from voltdb import FastSerializer, VoltProcedure

client = FastSerializer("192.168.58.3", 21212)
proc = VoltProcedure(client, "TEST2.insert", [FastSerializer.VOLTTYPE_INTEGER, FastSerializer.VOLTTYPE_STRING])
response = proc.call([1, "English"])
client.close()

print response
