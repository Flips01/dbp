package dbp;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public class SelectActiveConnections extends VoltProcedure{

    public VoltTable[] run(Timestamp timestamp){
        final SQLStmt singleSelect = new SQLStmt("SELECT SRC_IP, SRC_PORT, DST_IP, DST_PORT FROM Connections "
                +"WHERE Timestamp_Range = ? AND Timestamp between ? AND ?");
        LocalDateTime before = timestamp.toLocalDateTime().minusMinutes(1);
        LocalDateTime after = timestamp.toLocalDateTime().plusMinutes(1);
        if(before.getHour() != after.getHour()){
            voltQueueSQL(singleSelect, timestamp.getTime()/(1000*60*60),before, after);
        }else{
            long beforeTime = timestamp.getTime()-60*1000;
            long afterTime = timestamp.getTime()+60*1000;
            voltQueueSQL(singleSelect, beforeTime/(1000*60*60),before, timestamp);
            voltQueueSQL(singleSelect, afterTime/(1000*60*60), timestamp, afterTime);
        }
        return voltExecuteSQL();
    }
}
