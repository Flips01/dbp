package dbp.insert;

import de.hsh.WKPQuery;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

import java.sql.Timestamp;

public class InsertWellKnownPort extends VoltProcedure {

    public final SQLStmt insertWellKnownPort = new SQLStmt("INSERT INTO WELL_KNOWN_PORTS VALUES(?, ?)");


    public long run(Timestamp time, String srcIP, String dstIP, int srcPort, int dstPort, String flag, String payload, String type, int size) {
        final long START_TIME = System.currentTimeMillis();

        WKPQuery wkp = WKPQuery.getInstance();
        if(wkp.isWKP(dstPort)){
            voltQueueSQL(insertWellKnownPort, dstIP, dstPort);
        }

        voltExecuteSQL();

        return System.currentTimeMillis() - START_TIME;
    }
}
