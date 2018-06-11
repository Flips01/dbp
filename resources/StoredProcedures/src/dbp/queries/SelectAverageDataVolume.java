package dbp.queries;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public class SelectAverageDataVolume extends VoltProcedure{
    public final SQLStmt select = new SQLStmt("select PAYLOAD_SUM, PAYLOAD_COUNT from AVERAGE_DATA_VOLUME WHERE IP_A = ? AND IP_B = ?");

    public VoltTable run(String IP_A, String IP_B){
        voltQueueSQL(select, IP_A, IP_B);
        VoltTable numbers = voltExecuteSQL()[0];
        VoltTable result = new VoltTable(new VoltTable.ColumnInfo("Average", VoltType.FLOAT));
        try{
            final float PAYLOAD_COUNT = (float)numbers.fetchRow(0).getLong(1);
            if(PAYLOAD_COUNT > 0){
                result.addRow(numbers.fetchRow(0).getLong(0)/PAYLOAD_COUNT);
            }
        }catch (IndexOutOfBoundsException ioobe){

        }
        return result;
    }
}
