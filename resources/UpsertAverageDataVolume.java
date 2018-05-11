package dbp;
import org.voltdb.*;
public class UpsertAverageDataVolume extends VoltProcedure {

    public final SQLStmt findOld = new SQLStmt("SELECT * from AVERAGE_DATA_VOLUME where IP_A = ? and IP_B = ?");
    public final SQLStmt upsert = new SQLStmt("UPSERT INTO AVERAGE_DATA_VOLUME(IP_A, IP_B, PAYLOAD_SUM, PAYLOAD_COUNT) VALUES(?, ?, ?, ?)");

    public org.voltdb.VoltTable[] run(String srcIP, String dstIP, int size){
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
        return voltExecuteSQL();
    }
}
