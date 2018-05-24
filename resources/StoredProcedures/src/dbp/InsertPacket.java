package dbp;

import de.hsh.WKPQuery;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

public class InsertPacket extends VoltProcedure {

    public final SQLStmt insertActiveConnections = new SQLStmt("INSERT INTO CONNECTIONS VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
    public final SQLStmt insertPayload = new SQLStmt("INSERT INTO PAYLOAD VALUES(?, ?, ?)");
    public final SQLStmt insertPortConnection = new SQLStmt("INSERT INTO PORT_CONNECTIONS VALUES(?, ?)");
    public final SQLStmt insertWellKnownPort = new SQLStmt("INSERT INTO WELL_KNOWN_PORTS VALUES(?, ?)");

    public final SQLStmt findOld = new SQLStmt("SELECT * from AVERAGE_DATA_VOLUME where IP_A = ? and IP_B = ?");
    public final SQLStmt upsert = new SQLStmt("UPSERT INTO AVERAGE_DATA_VOLUME(IP_A, IP_B, PAYLOAD_SUM, PAYLOAD_COUNT) VALUES(?, ?, ?, ?)");


    public long run(Timestamp time, String srcIP, String dstIP, int srcPort, int dstPort, String flag, String payload, String type, int size){
        long startTime = System.currentTimeMillis();

        upsertAverageDataVolume(srcIP, dstIP, size);

        String packetID = UUID.randomUUID().toString();

        LocalDateTime timeLocal = time.toLocalDateTime();
        timeLocal = timeLocal.minusMinutes(timeLocal.getMinute()).minusSeconds(timeLocal.getSecond()).minusNanos(timeLocal.getNano());
        //Dangerous time magic
        Timestamp hour = Timestamp.from(timeLocal.toInstant(ZoneOffset.ofHours(0)));
        voltQueueSQL(insertActiveConnections, hour.getNanos(), packetID, time, srcIP, dstIP, srcPort, dstPort, flag);
        voltQueueSQL(insertPayload, hour.getNanos(), packetID, payload);
        voltQueueSQL(insertPortConnection, dstIP+":"+dstPort,srcIP);

        WKPQuery wkp = WKPQuery.getInstance();
        if(wkp.isWKP(dstPort)){
	    try {
            	voltQueueSQL(insertWellKnownPort, dstIP, dstPort);
	    } catch(Exception e) {
	    }
        }

        voltExecuteSQL();

        return System.currentTimeMillis()-startTime;
    }

    public boolean isPortWellKnown(int port){
        return port < 1024; //TODO
    }

    public void upsertAverageDataVolume(String srcIP, String dstIP, int size){
        voltQueueSQL(findOld, srcIP, dstIP);
        VoltTable[] results = voltExecuteSQL();
        if(results[0].getRowCount() > 0){
            VoltTableRow row = results[0].fetchRow(0);
            long oldCount =  row.getLong("PAYLOAD_COUNT");
            long oldSum = row.getLong("PAYLOAD_SUM");
            voltQueueSQL(upsert, srcIP, dstIP, oldSum+size, oldCount+1);
        }else{
            voltQueueSQL(upsert, srcIP, dstIP, size, 1);
        }
        voltExecuteSQL();
    }
}
