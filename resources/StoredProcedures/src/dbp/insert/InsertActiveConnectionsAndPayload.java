package dbp.insert;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

public class InsertActiveConnectionsAndPayload extends VoltProcedure{

    public final SQLStmt insertActiveConnections = new SQLStmt("INSERT INTO CONNECTIONS VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
    public final SQLStmt insertPayload = new SQLStmt("INSERT INTO PAYLOAD VALUES(?, ?, ?)");


    public long run(Timestamp time, String srcIP, String dstIP, int srcPort, int dstPort, String flag, String payload){

        final long START_TIME = System.currentTimeMillis();

        LocalDateTime timeLocal = time.toLocalDateTime();

        timeLocal = timeLocal.minusMinutes(timeLocal.getMinute()).minusSeconds(timeLocal.getSecond()).minusNanos(timeLocal.getNano());
        //Dangerous time magic
        Timestamp hour = Timestamp.from(timeLocal.toInstant(ZoneOffset.ofHours(0)));

        String packetID = UUID.randomUUID().toString();


        voltQueueSQL(insertActiveConnections, hour.getTime()/(1000*60*60), packetID, time, srcIP, dstIP, srcPort, dstPort, flag);
        voltQueueSQL(insertPayload, hour.getTime()/(1000*60*60), packetID, payload);

        voltExecuteSQL();

        return System.currentTimeMillis()-START_TIME;
    }
}
