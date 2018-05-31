package dbp.insert;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

public class UpsertAverageDataVolume extends VoltProcedure{

    public final SQLStmt findOld = new SQLStmt("SELECT * from AVERAGE_DATA_VOLUME where IP_A = ? and IP_B = ?");
    public final SQLStmt upsert = new SQLStmt("UPSERT INTO AVERAGE_DATA_VOLUME(IP_A, IP_B, PAYLOAD_SUM, PAYLOAD_COUNT) VALUES(?, ?, ?, ?)");


    public long run(String srcIP, String dstIP, int size){
        final long START_TIME = System.currentTimeMillis();

        if(srcIP.compareTo(dstIP) == 1){
            String tmp = srcIP;
            srcIP = dstIP;
            dstIP = tmp;
        }

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

        return System.currentTimeMillis()-START_TIME;
    }
}
