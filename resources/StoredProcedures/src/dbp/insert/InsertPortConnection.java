package dbp.insert;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

import java.sql.Timestamp;

public class InsertPortConnection extends VoltProcedure {

    public final SQLStmt insertPortConnection = new SQLStmt("INSERT INTO PORT_CONNECTIONS VALUES(?, ?)");


    public long run(String srcIP, String dstIP, int dstPort){
        final long START_TIME = System.currentTimeMillis();

        voltQueueSQL(insertPortConnection, dstIP+":"+dstPort,srcIP);
        voltExecuteSQL();

        return System.currentTimeMillis()-START_TIME;
    }
}
