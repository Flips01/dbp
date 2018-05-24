package dbp;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * Created by badent on 24.05.18.
 */

public class query3 extends VoltProcedure {
    /**
     * Query: 3
     * Retrieve all hosts that had connections to IP a.b.c.d on a given port number
     */

    public final SQLStmt selectQuery = new SQLStmt("SELECT * FROM PORT_CONNECTIONS WHERE DST_IP_PORT = ?");

    public VoltTable[] run(String dstIP, int dstPort){
        voltQueueSQL(selectQuery, dstIP+ ":" + dstPort);
        return voltExecuteSQL();
    }

}